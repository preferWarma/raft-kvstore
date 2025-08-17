// CloudUploader.h
#pragma once
#include "Config.h"
#include "HttpClient.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

namespace lyf {

class CloudUploader {
public:
  inline CloudUploader()
      : config_(Config::GetInstance()),
        httpClient_(std::make_unique<HttpClient>()) {}

  inline ~CloudUploader() { stop(); }

  // 启动上传服务
  inline void start() {
    if (running_.exchange(true)) {
      return; // 已经在运行
    }

    shouldStop_ = false;
    uploadWorker_ = std::thread(&CloudUploader::uploadWorkerLoop, this);

    lyf_Internal_LOG("[CloudUploader] Upload service started");
  }

  // 停止上传服务
  inline void stop() {
    if (!running_.exchange(false)) {
      return; // 已经停止
    }

    shouldStop_ = true;
    queueCondition_.notify_all();

    if (uploadWorker_.joinable()) {
      uploadWorker_.join();
    }

    lyf_Internal_LOG("[CloudUploader] Upload service stopped");
  }

  // 添加文件到上传队列
  inline bool scheduleUpload(const std::string &filePath) {
    if (!running_ || !validateFile(filePath)) {
      return false;
    }

    std::lock_guard<std::mutex> lock(queueMutex_);

    if (uploadQueue_.size() >= config_.cloud.maxQueueSize) {
      lyf_Internal_LOG("[CloudUploader] Upload queue is full, dropping file: {}", filePath);
      return false;
    }

    uploadQueue_.emplace(filePath);
    queueCondition_.notify_one();

    lyf_Internal_LOG("[CloudUploader] Scheduled upload for: {}", filePath);
    return true;
  }

  // 立即上传文件（同步方式）
  inline bool uploadFileSync(const std::string &filePath) {
    if (!validateFile(filePath)) {
      return false;
    }

    UploadTask task(filePath);
    return performUpload(task);
  }

  // 获取队列状态
  inline size_t getQueueSize() const {
    std::lock_guard<std::mutex> lock(queueMutex_);
    return uploadQueue_.size();
  }

  inline bool isRunning() const { return running_.load(); }

  // 测试服务器连通性
  inline bool ping() const {
    std::string baseUrl = config_.cloud.serverUrl;
    // 确保baseUrl以/结尾
    if (!baseUrl.empty() && baseUrl.back() != '/') {
      baseUrl += '/';
    }

    std::string endpoint = "ping";
    std::string url = baseUrl + endpoint;

    auto result = httpClient_->get(url, config_.cloud.apiKey);
    return result.success;
  }

private:
  struct UploadTask {
    std::string filePath;
    int retryCount = 0;
    std::chrono::system_clock::time_point nextRetryTime;

    UploadTask(const std::string &path)
        : filePath(path), nextRetryTime(std::chrono::system_clock::now()) {}
  };

private:
  Config &config_;
  std::unique_ptr<HttpClient> httpClient_;

  std::queue<UploadTask> uploadQueue_;
  mutable std::mutex queueMutex_;
  std::condition_variable queueCondition_;

  std::thread uploadWorker_;
  std::atomic<bool> running_{false};
  std::atomic<bool> shouldStop_{false};

private:
  // 工作线程主循环
  inline void uploadWorkerLoop() {
    while (true) {
      std::unique_lock<std::mutex> lock(queueMutex_);

      // 等待队列有任务或收到停止信号
      queueCondition_.wait(
          lock, [this] { return !uploadQueue_.empty() || shouldStop_; });

      // 如果收到停止信号且队列已空，则退出
      if (shouldStop_ && uploadQueue_.empty()) {
        break;
      }

      // 处理队列中的任务
      std::queue<UploadTask> retryQueue;

      while (!uploadQueue_.empty()) {
        UploadTask task = std::move(uploadQueue_.front());
        uploadQueue_.pop();

        lock.unlock(); // 释放锁进行上传操作

        // 检查是否到了重试时间
        auto now = std::chrono::system_clock::now();
        if (now < task.nextRetryTime) {
          retryQueue.push(task);
          lock.lock();
          continue;
        }

        bool success = performUpload(task);

        if (!success && task.retryCount < config_.cloud.maxRetries) {
          task.retryCount++;
          task.nextRetryTime =
              now + std::chrono::seconds(config_.cloud.retryDelay_s);
          retryQueue.push(task);

          lyf_Internal_LOG("[CloudUploader] Upload failed, scheduling retry {}/{} for: {}", task.retryCount, config_.cloud.maxRetries, task.filePath);
        } else if (!success) {
          lyf_Internal_LOG("[CloudUploader] Upload failed permanently after {} retries: {}", config_.cloud.maxRetries, task.filePath);
        }

        lock.lock();
      }

      // 将需要重试的任务重新加入队列
      while (!retryQueue.empty()) {
        uploadQueue_.push(std::move(retryQueue.front()));
        retryQueue.pop();
      }
    }

    lyf_Internal_LOG("[CloudUploader] Worker thread exiting");
  }

  // 执行单个文件上传
  inline bool performUpload(const UploadTask &task) {
    std::string baseUrl = config_.cloud.serverUrl;
    // 确保baseUrl以/结尾
    if (!baseUrl.empty() && baseUrl.back() != '/') {
      baseUrl += '/';
    }

    std::string endpoint = config_.cloud.uploadEndpoint;
    // 确保endpoint不以/开头
    if (!endpoint.empty() && endpoint.front() == '/') {
      endpoint = endpoint.substr(1);
    }

    std::string url = baseUrl + endpoint;

    auto result = httpClient_->uploadFile(url, task.filePath, "file",
                                          config_.cloud.apiKey);

    if (result.success) {
      logUploadResult(task.filePath, true, "Upload successful");

      // 如果配置了上传后删除，则删除本地文件
      if (config_.cloud.deleteAfterUpload) {
        try {
          std::filesystem::remove(task.filePath);
          lyf_Internal_LOG("[CloudUploader] Deleted local file: {}", task.filePath);
        } catch (const std::exception &e) {
          lyf_Internal_LOG("[CloudUploader] Failed to delete local file {}: {}", task.filePath, e.what());
        }
      }

      return true;
    } else {
      std::string errorMsg = "HTTP " + std::to_string(result.statusCode) +
                             " - " + result.errorMessage;
      logUploadResult(task.filePath, false, errorMsg);
      return false;
    }
  }

  // 验证文件是否存在且可读
  inline bool validateFile(const std::string &filePath) const {
    try {
      return std::filesystem::exists(filePath) &&
             std::filesystem::is_regular_file(filePath) &&
             std::filesystem::file_size(filePath) > 0;
    } catch (const std::exception &e) {
      lyf_Internal_LOG("[CloudUploader] File validation error: {}", e.what());
      return false;
    }
  }

  // 记录上传结果
  inline void logUploadResult(const std::string &filePath, bool success,
                              const std::string &message) {
    if (success) {
      lyf_Internal_LOG("[CloudUploader] ✓ Upload successful: {}", filePath);
    } else {
      lyf_Internal_LOG("[CloudUploader] ✗ Upload failed: {} - {}", filePath, message);
    }
  }
};

} // namespace lyf