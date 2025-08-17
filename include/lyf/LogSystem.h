#pragma once

#include "Config.h"
#include "FastFormater.h"
#include "Helper.h"
#include "LogQue.h"
#include "Singleton.h"

#if defined(CLOUD_INCLUDE)
#include "CloudUploader.h"
#endif

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <ostream>
#include <utility>

// 日志调用宏
#if _LIBCPP_STD_VER >= 20
#define LOG_DEBUG(fmt, ...)                                                    \
  lyf::AsyncLogSystem::GetInstance().Faster_Debug<fmt>(__VA_ARGS__)
#define LOG_INFO(fmt, ...)                                                     \
  lyf::AsyncLogSystem::GetInstance().Faster_Info<fmt>(__VA_ARGS__)
#define LOG_WARN(fmt, ...)                                                     \
  lyf::AsyncLogSystem::GetInstance().Faster_Warn<fmt>(__VA_ARGS__)
#define LOG_ERROR(fmt, ...)                                                    \
  lyf::AsyncLogSystem::GetInstance().Faster_Error<fmt>(__VA_ARGS__)
#define LOG_FATAL(fmt, ...)                                                    \
  lyf::AsyncLogSystem::GetInstance().Faster_Fatal<fmt>(__VA_ARGS__)
#else
#define LOG_DEBUG(...) lyf::AsyncLogSystem::GetInstance().Debug(__VA_ARGS__)
#define LOG_INFO(...) lyf::AsyncLogSystem::GetInstance().Info(__VA_ARGS__)
#define LOG_WARN(...) lyf::AsyncLogSystem::GetInstance().Warn(__VA_ARGS__)
#define LOG_ERROR(...) lyf::AsyncLogSystem::GetInstance().Error(__VA_ARGS__)
#define LOG_FATAL(...) lyf::AsyncLogSystem::GetInstance().Fatal(__VA_ARGS__)
#endif

namespace lyf {
using steady_clock = std::chrono::steady_clock;
using std::unique_ptr, std::make_unique, std::mutex, std::lock_guard;

class AsyncLogSystem : public Singleton<AsyncLogSystem> {
  friend class Singleton<AsyncLogSystem>;

private:
  AsyncLogSystem() : config_(Config::GetInstance()) { Init(); }

public:
  inline void Init();
  inline void Stop();
  inline void Flush();
  ~AsyncLogSystem() noexcept { Stop(); }

public:
  template <typename... Args>
  inline void Log(LogLevel level, const string &fmt, Args &&...args);

  template <typename... Args>
  inline void Debug(const string &fmt, Args &&...args) {
    Log(LogLevel::DEBUG, fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline void Info(const string &fmt, Args &&...args) {
    Log(LogLevel::INFO, fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline void Warn(const string &fmt, Args &&...args) {
    Log(LogLevel::WARN, fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline void Error(const string &fmt, Args &&...args) {
    Log(LogLevel::ERROR, fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline void Fatal(const string &fmt, Args &&...args) {
    Log(LogLevel::FATAL, fmt, std::forward<Args>(args)...);
  }

#if _LIBCPP_STD_VER >= 20
public: // 编译期优化(C++20级以上支持)
  template <FixedString fmt, typename... Args>
  inline void Faster_Log(LogLevel level, Args &&...args);

  template <FixedString fmt, typename... Args>
  inline void Faster_Debug(Args &&...args) {
    Faster_Log<fmt>(LogLevel::DEBUG, std::forward<Args>(args)...);
  }

  template <FixedString fmt, typename... Args>
  inline void Faster_Info(Args &&...args) {
    Faster_Log<fmt>(LogLevel::INFO, std::forward<Args>(args)...);
  }

  template <FixedString fmt, typename... Args>
  inline void Faster_Warn(Args &&...args) {
    Faster_Log<fmt>(LogLevel::WARN, std::forward<Args>(args)...);
  }

  template <FixedString fmt, typename... Args>
  inline void Faster_Error(Args &&...args) {
    Faster_Log<fmt>(LogLevel::ERROR, std::forward<Args>(args)...);
  }

  template <FixedString fmt, typename... Args>
  inline void Faster_Fatal(Args &&...args) {
    Faster_Log<fmt>(LogLevel::FATAL, std::forward<Args>(args)...);
  }

#endif

public:
  inline string getCurrentLogFilePath() const { return currentLogFilePath_; }

  inline void setRotationType(RotationType type) { rotationType_ = type; }

  inline void setMaxFileSize(size_t maxSize) {
    config_.rotation.maxFileSize = maxSize;
  }

  inline void setMaxFileCount(int maxCount) {
    config_.rotation.maxFileCount = maxCount;
  }

  // 手动触发轮转
  inline void forceRotation();

  // 获取日志目录中的所有日志文件
  inline std::vector<std::string> getLogFiles() const;

private:
  // 生成新的日志文件名
  inline std::string generateLogFileName() const;

  // 生成按日期的日志文件名 (用于按时间轮转)
  inline std::string
  generateDailyLogFileName(const std::string &date = "") const;

  inline bool initializeLogFile();  // 初始化日志文件
  inline bool needsRotation();      // 检查是否需要轮转
  inline bool rotateLogFile();      // 执行日志轮转
  inline void cleanupOldLogFiles(); // 清理旧的日志文件

public:
#if defined(CLOUD_INCLUDE)
  // 获取上传队列状态
  inline size_t getUploadQueueSize() const {
    return cloudUploader_ ? cloudUploader_->getQueueSize() : 0;
  }

  inline const auto &getCloudUploader() const { return cloudUploader_; }

  // 检查云端上传是否启用
  inline bool isCloudUploadEnabled() const {
    return config_.cloud.enable && cloudUploader_ != nullptr;
  }
#endif

private:
  // 控制台输出线程
  inline void ConsoleWorkerLoop();
  inline void FileWorkerLoop();
  inline void ProcessConsoleBatch(LogQueue::QueueType &batchQueue,
                                  std::string &buffer);
  inline void ProcessFileBatch(LogQueue::QueueType &batchQueue,
                               std::string &buffer);

private:
  Config &config_; // 配置引用

private:
  std::ofstream logFile_;                      // 当前的日志文件输出流
  atomic<bool> isStop_{true};                  // 是否关闭
  steady_clock::time_point lastFileFlushTime_; // 上次文件刷新时间
  mutable mutex fileMtx_;                      // 保证文件写入不会乱序
  mutable std::mutex flushSerializerMtx_; // 保证刷新操作的原子性发起用于
  mutable std::mutex flushMtx_;           //  完成等待 时的线程同步
  std::condition_variable flushCv_; // 用于等待刷新完成的线程同步
  std::atomic<int> flushRequestCount_{0}; // 刷新请求计数(console/file)
  const std::string FLUSH_COMMAND_ = "__FLUSH_COMMAND__"; // 刷新命令

private:
  thread consoleWorker_;            // 控制台输出线程
  thread fileWorker_;               // 文件输出线程
  unique_ptr<LogQueue> consoleQue_; // 控制台输出队列
  unique_ptr<LogQueue> fileQue_;    // 文件输出队列

private:
  // 日志文件轮转相关
  string currentLogFilePath_; // 当前日志文件完整路径
  RotationType rotationType_; // 轮转类型
  string lastRotationDate_;   // 上次轮转的日期(用于按时间轮转)
  atomic<bool> isRotating_;   // 是否正在轮转
  atomic<int> rotateCounts_;  // 轮转次数
  atomic<size_t> currentFileWrittenBytes_{0}; // 当前文件已写入字节数

#if defined(CLOUD_INCLUDE)
private:
  unique_ptr<CloudUploader> cloudUploader_; // 云上传器
#endif

}; // class AsyncLogSystem

inline void AsyncLogSystem::Init() {
  // 还在运行, 不初始化
  if (!isStop_.exchange(false)) {
    return;
  }
  consoleQue_ = make_unique<LogQueue>(config_.basic.maxQueueSize);
  fileQue_ = make_unique<LogQueue>(config_.basic.maxQueueSize);
  rotationType_ = RotationType::BY_SIZE_AND_TIME;
  lastFileFlushTime_ = std::chrono::steady_clock::now();
  rotateCounts_ = 0;

  if (config_.output.toFile) {
    if (initializeLogFile()) {
      lyf_Internal_LOG(
          "[LogSystem] Log system initialized. Current log file: {}",
          currentLogFilePath_);
    } else {
      lyf_Internal_LOG("[LogSystem] Failed to initialize log file.");
      config_.output.toFile = false;
    }
  }

  if (config_.cloud.enable) {
#if defined(CLOUD_INCLUDE)
    cloudUploader_ = make_unique<CloudUploader>();
    cloudUploader_->start();

    if (!cloudUploader_->ping()) {
      lyf_Internal_LOG(
          "[LogSystem] Cloud upload enabled, serverUrl: {} is not available.",
          config_.cloud.serverUrl);
      config_.cloud.enable = false;
    } else {
      lyf_Internal_LOG("[LogSystem] Cloud upload enabled, serverUrl: {}",
                       config_.cloud.serverUrl);
    }
#endif
  }

  // 启动控制台输出线程
  if (config_.output.toConsole) {
    consoleWorker_ = thread(&AsyncLogSystem::ConsoleWorkerLoop, this);
  }
  // 启动文件输出线程
  if (config_.output.toFile) {
    fileWorker_ = thread(&AsyncLogSystem::FileWorkerLoop, this);
  }
}

inline void AsyncLogSystem::Stop() {
  bool expected = false;
  if (!isStop_.compare_exchange_strong(expected, true)) {
    return;
  }

  consoleQue_->Stop();
  fileQue_->Stop();

  // 等待工作线程完成
  if (consoleWorker_.joinable()) {
    consoleWorker_.join();
  }
  if (fileWorker_.joinable()) {
    fileWorker_.join();
  }

  // 最终flush
  if (config_.output.toFile) {
    lock_guard<mutex> lock(fileMtx_);
    if (logFile_.is_open()) {
      logFile_.flush();
    }
  }
  // 停止云端上传器
#if defined(CLOUD_INCLUDE)
  if (cloudUploader_) {
    cloudUploader_->stop();
  }
#endif

  if (logFile_.is_open()) {
    logFile_.close();
  }

  // 输出系统关闭信息到控制台
  if (config_.output.toConsole) {
    // 红色字体
    lyf_Internal_LOG("[LogSystem] Log system closed.");
  }
}

inline void AsyncLogSystem::Flush() {
  std::lock_guard<std::mutex> serializer_lock(flushSerializerMtx_);
  if (isStop_.load(std::memory_order_relaxed)) {
    return;
  }

  std::unique_lock<std::mutex> flush_lock(flushMtx_);

  int active_workers = 0;
  LogMessage flush_cmd(LogLevel::INFO, FLUSH_COMMAND_);

  if (config_.output.toConsole && consoleQue_) {
    active_workers++;
    consoleQue_->Push(flush_cmd);
  }
  if (config_.output.toFile && fileQue_) {
    active_workers++;
    fileQue_->Push(flush_cmd);
  }

  if (active_workers == 0) {
    return;
  }

  flushRequestCount_.store(active_workers);

  // 等待所有工作线程处理完刷新命令
  flushCv_.wait(flush_lock, [this] { return flushRequestCount_.load() == 0; });
}

template <typename... Args>
inline void AsyncLogSystem::Log(LogLevel level, const string &fmt,
                                Args &&...args) {
  if (isStop_ || static_cast<int>(level) < config_.output.minLogLevel) {
    return;
  }
  auto msg = LogMessage(level, FormatMessage(fmt, std::forward<Args>(args)...));

  // 分发到不同对列
  if (config_.output.toConsole) {
    consoleQue_->Push(LogMessage(msg)); // 复制一份给控制台
  }
  if (config_.output.toFile) {
    fileQue_->Push(std::move(msg)); // 直接移动给文件队列,提高性能
  }
}

inline void AsyncLogSystem::forceRotation() {
  if (rotateLogFile()) {
    lyf_Internal_LOG("[LogSystem] Manual log rotation completed. New file: {}",
                     currentLogFilePath_);
  }
}

inline std::vector<std::string> AsyncLogSystem::getLogFiles() const {
  std::vector<std::string> files;
  try {
    for (const auto &entry :
         std::filesystem::directory_iterator(config_.output.logRootDir)) {
      if (entry.is_regular_file()) {
        std::string filename = entry.path().filename().string();
        if (StartWith(filename, "log_") && EndWith(filename, ".log")) {
          files.push_back(entry.path().string());
        }
      }
    }

    // 按文件名排序
    std::sort(files.begin(), files.end());
  } catch (const std::exception &e) {
    std::cerr << "[LogSystem] Error listing log files: " << e.what()
              << std::endl;
  }
  return files;
}

inline std::string AsyncLogSystem::generateLogFileName() const {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  std::stringstream ss;
  ss << "log_" << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S_")
     << rotateCounts_ << ".log";
  return (std::filesystem::path(config_.output.logRootDir) / ss.str()).string();
}

// 生成按日期的日志文件名 (用于按时间轮转)
inline std::string
AsyncLogSystem::generateDailyLogFileName(const std::string &date) const {
  std::string dateStr = date;
  if (dateStr.empty()) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y%m%d");
    dateStr = ss.str();
  }
  return (std::filesystem::path(config_.output.logRootDir) /
          ("log_" + dateStr + ".log"))
      .string();
}

inline bool AsyncLogSystem::initializeLogFile() {
  try {
    // 创建日志目录
    if (!std::filesystem::exists(config_.output.logRootDir)) {
      std::filesystem::create_directories(config_.output.logRootDir);
      lyf_Internal_LOG("[LogSystem] Created log directory: {}",
                       config_.output.logRootDir);
    }
    // 根据轮转类型生成文件名
    if (rotationType_ == RotationType::BY_TIME ||
        rotationType_ == RotationType::BY_SIZE_AND_TIME) {
      currentLogFilePath_ = generateDailyLogFileName();
      lastRotationDate_ = GetCurrentTime("%Y%m%d");
    } else {
      currentLogFilePath_ = generateLogFileName();
    }
    // 打开日志文件
    logFile_.open(currentLogFilePath_, std::ios::app | std::ios::out);
    if (!logFile_.is_open()) {
      std::cerr << "[LogSystem] Failed to open log file: "
                << currentLogFilePath_ << std::endl;
      return false;
    }
    logFile_.flush();
    currentFileWrittenBytes_ = std::filesystem::file_size(currentLogFilePath_);
    return true;
  } catch (const std::exception &e) {
    std::cerr << "[LogSystem] Error initializing log file: " << e.what()
              << std::endl;
    return false;
  }
}

inline bool AsyncLogSystem::needsRotation() {
  if (!config_.output.toFile || !logFile_.is_open()) {
    return false;
  }
  try {
    bool needRotation = false;
    // 检查文件大小
    if (rotationType_ == RotationType::BY_SIZE ||
        rotationType_ == RotationType::BY_SIZE_AND_TIME) {
      if (currentFileWrittenBytes_.load(std::memory_order_relaxed) >=
          config_.rotation.maxFileSize) {
        needRotation = true;
      }
    }
    // 检查日期变化
    if (rotationType_ == RotationType::BY_TIME ||
        rotationType_ == RotationType::BY_SIZE_AND_TIME) {
      std::string currentDate = GetCurrentTime("%Y%m%d");
      if (currentDate != lastRotationDate_) {
        needRotation = true;
      }
    }
    return needRotation;
  } catch (const std::exception &e) {
    std::cerr << "[LogSystem] Error checking rotation needs: " << e.what()
              << std::endl;
    return false;
  }
}

inline bool AsyncLogSystem::rotateLogFile() {
  if (!config_.output.toFile) {
    return true;
  }
  // 使用原子标志避免重复轮转
  bool expected = false;
  if (!isRotating_.compare_exchange_strong(expected, true)) {
    return false; // 已经在轮转中
  }
  FlagGuard guard(isRotating_);

  try {
    std::lock_guard<std::mutex> lock(fileMtx_);
    // 写入轮转信息到当前文件
    if (logFile_.is_open()) {
      logFile_.flush();
      logFile_.close();
    }
    // 生成新的日志文件名
    std::string newLogFile;
    if (rotationType_ == RotationType::BY_TIME ||
        rotationType_ == RotationType::BY_SIZE_AND_TIME) {
      std::string currentDate = GetCurrentTime("%Y%m%d");
      if (currentDate != lastRotationDate_) {
        // 按日期轮转
        newLogFile = generateDailyLogFileName(currentDate);
        lastRotationDate_ = currentDate;
      } else {
        // 同一天内按大小轮转，添加序号
        newLogFile = generateLogFileName();
      }
    } else {
      newLogFile = generateLogFileName();
    }

    string oldLogFilePath = currentLogFilePath_;
    currentLogFilePath_ = newLogFile;
    // 打开新的日志文件
    logFile_.open(currentLogFilePath_, std::ios::app | std::ios::out);
    if (!logFile_.is_open()) {
      std::cerr << "[LogSystem] Failed to open new log file: "
                << currentLogFilePath_ << std::endl;
      return false;
    }
    logFile_.flush();
#if defined(CLOUD_INCLUDE)
    // 上传旧日志文件
    if (std::filesystem::exists(oldLogFilePath) && isCloudUploadEnabled()) {
      bool success = cloudUploader_->scheduleUpload(oldLogFilePath);
      if (success) {
        if (config_.cloud.deleteAfterUpload) {
          std::filesystem::remove(oldLogFilePath);
        }
        lyf_Internal_LOG("[LogSystem] Uploaded old log file: {}",
                         oldLogFilePath);
      } else {
        lyf_Internal_LOG("[LogSystem] Failed to upload old log file: {}",
                         oldLogFilePath);
      }
    }
#endif
    // 清理旧日志文件
    cleanupOldLogFiles();
    lyf_Internal_LOG("[LogSystem] Old log files cleaned up.");
    // 增加轮转次数
    ++rotateCounts_;
    currentFileWrittenBytes_.store(0, std::memory_order_relaxed);
    return true;

  } catch (const std::exception &e) {
    std::cerr << "[LogSystem] Error during log rotation: " << e.what()
              << std::endl;
    return false;
  }
}

inline void AsyncLogSystem::cleanupOldLogFiles() {
  try {
    std::vector<std::filesystem::path> logFiles;
    // 收集所有日志文件
    for (const auto &entry :
         std::filesystem::directory_iterator(config_.output.logRootDir)) {
      if (entry.is_regular_file()) {
        std::string filename = entry.path().filename().string();
        if (filename.starts_with("log_") && filename.ends_with(".log")) {
          logFiles.push_back(entry.path());
        }
      }
    }
    // 按修改时间排序（最新的在前）
    std::sort(logFiles.begin(), logFiles.end(),
              [](const auto &a, const auto &b) {
                return std::filesystem::last_write_time(a) >
                       std::filesystem::last_write_time(b);
              });
    // 删除超出数量限制的文件
    for (size_t i = config_.rotation.maxFileCount; i < logFiles.size(); ++i) {
      try {
        std::filesystem::remove(logFiles[i]);
        lyf_Internal_LOG("[LogSystem] Removed old log file: {}",
                         logFiles[i].c_str());
      } catch (const std::exception &e) {
        lyf_Internal_LOG("[LogSystem] Failed to remove old log file {}: {}",
                         logFiles[i].c_str(), e.what());
      }
    }
  } catch (const std::exception &e) {
    std::cerr << "[LogSystem] Error during log cleanup: {}\n" << e.what();
  }
}

inline void AsyncLogSystem::ConsoleWorkerLoop() {
  LogQueue::QueueType batchQueue; // 当前批次的日志消息队列
  std::string buffer;
  buffer.reserve(1024 *
                 config_.performance.consoleBufferSize_kb); // 预分配缓冲区

  auto work = [&]() {
    ProcessConsoleBatch(batchQueue, buffer);
    batchQueue = LogQueue::QueueType();
    buffer.clear();
  };

  int sleepTime_ms = 1;

  while (!isStop_.load(std::memory_order_relaxed)) {
    if (consoleQue_->PopBatch(batchQueue,
                              config_.performance.consoleBatchSize) > 0) {
      work();
      sleepTime_ms = 1;
    } else {
      // 队列为空，短暂休眠避免忙等待
      std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime_ms));
      sleepTime_ms = std::min(100, sleepTime_ms * 2); // 指数退避算法
    }
  }
  // 处理剩余的日志消息
  while (consoleQue_->PopBatch(batchQueue) > 0) {
    work();
  }
}

inline void AsyncLogSystem::FileWorkerLoop() {
  LogQueue::QueueType batchQueue; // 当前批次的日志消息队列
  std::string buffer;
  buffer.reserve(1024 * config_.performance.fileBufferSize_kb); // 预分配缓冲区

  auto work = [&]() {
    ProcessFileBatch(batchQueue, buffer);
    batchQueue = LogQueue::QueueType();
    buffer.clear();
  };

  int sleepTime_ms = 1;

  while (!isStop_.load(std::memory_order_relaxed)) {
    if (fileQue_->PopBatch(batchQueue, config_.performance.fileBatchSize) > 0) {
      work();
      sleepTime_ms = 1;
    } else {
      // 队列为空，短暂休眠避免忙等待
      std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime_ms));
      sleepTime_ms = std::min(100, sleepTime_ms * 2); // 指数退避算法
    }
  }
  // 处理剩余的日志消息
  while (fileQue_->PopBatch(batchQueue) > 0) {
    work();
  }
}

inline void AsyncLogSystem::ProcessConsoleBatch(LogQueue::QueueType &batchQueue,
                                                std::string &buffer) {
  if (batchQueue.empty()) {
    return;
  }
  size_t processedCount = 0;
  while (!batchQueue.empty()) {
    auto msg = std::move(batchQueue.front());
    batchQueue.pop_front();

    // 刷新请求到来，立即刷新
    if (msg.content == FLUSH_COMMAND_) {
      if (!buffer.empty()) {
        std::cout << buffer << std::flush;
        buffer.clear();
      } else {
        std::cout.flush();
      }
      processedCount = 0;

      if (flushRequestCount_.fetch_sub(1) == 1) {
        // 刷新请求计数减到0，通知等待线程
        lock_guard<mutex> cv_lock(flushMtx_);
        flushCv_.notify_one();
      }
      continue;
    }

    buffer += LevelColor(msg.level);
    buffer +=
        "[" + FormatTime(msg.time) + "] [" + LevelToString(msg.level) + "] ";
    buffer += msg.content;
    buffer += "\033[0m\n";

    ++processedCount;
    // 按批次大小输出, 避免缓冲区过大
    if (processedCount >= config_.performance.consoleBatchSize) {
      if (config_.performance.enableAsyncConsole) {
        std::cout << buffer; // 异步输出, 不立即刷新
      } else {
        std::cout << buffer << std::flush;
      }
      buffer.clear();
      processedCount = 0;
    }
  }
  // 输出剩余内容
  if (!buffer.empty()) {
    std::cout << buffer << std::flush;
  }
}

inline void AsyncLogSystem::ProcessFileBatch(LogQueue::QueueType &batchQueue,
                                             std::string &buffer) {
  if (batchQueue.empty()) {
    return;
  }

  auto writeFile = [&](const char *data, size_t size) {
    if (!logFile_.is_open() || size == 0) {
      return;
    }
    lock_guard<mutex> lock(fileMtx_);
    logFile_.write(data, size);
    currentFileWrittenBytes_.fetch_add(size, std::memory_order_relaxed);
  };

  while (!batchQueue.empty()) {
    // 刷新请求到来，立即刷新
    if (batchQueue.front().content == FLUSH_COMMAND_) {
      batchQueue.pop_front();
      if (!buffer.empty()) {
        writeFile(buffer.data(), buffer.size());
        buffer.clear();
      }
      // 刷新文件流
      {
        lock_guard<mutex> lock(fileMtx_);
        if (logFile_.is_open()) {
          logFile_.flush();
        }
      }

      if (flushRequestCount_.fetch_sub(1) == 1) {
        // 刷新请求计数减到0，通知等待线程
        lock_guard<mutex> cv_lock(flushMtx_);
        flushCv_.notify_one();
      }
      continue;
    }

    if (needsRotation()) {
      if (!buffer.empty()) {
        writeFile(buffer.data(), buffer.size());
        buffer.clear();
      }
      if (rotateLogFile()) {
        lyf_Internal_LOG("[LogSystem] Log rotated to: {}", currentLogFilePath_);
      }
    }

    auto msg = std::move(batchQueue.front());
    batchQueue.pop_front();

    std::string msgStr = "[" + FormatTime(msg.time) + "] [" +
                         LevelToString(msg.level) + "] " + msg.content + "\n";

    // 避免缓冲区无限增长
    if (buffer.size() + msgStr.size() > 1024 * 64) {
      writeFile(buffer.data(), buffer.size());
      buffer.clear();
    }
    buffer += msgStr;
  }

  if (!buffer.empty()) {
    writeFile(buffer.data(), buffer.size());
    buffer.clear();
  }

  auto now = std::chrono::steady_clock::now();
  if (now - lastFileFlushTime_ >= config_.performance.fileFlushInterval) {
    lock_guard<mutex> lock(fileMtx_);
    if (logFile_.is_open()) {
      logFile_.flush();
    }
    lastFileFlushTime_ = now;
  }
}

#if _LIBCPP_STD_VER >= 20
template <FixedString fmt, typename... Args>
inline void AsyncLogSystem::Faster_Log(LogLevel level, Args &&...args) {
  if (isStop_ || static_cast<int>(level) < config_.output.minLogLevel) {
    return;
  }
  // 采用编译期格式化版本
  auto msg = LogMessage(level, FormatMessage<fmt>(std::forward<Args>(args)...));

  // 分发到不同对列
  if (config_.output.toConsole) {
    consoleQue_->Push(LogMessage(msg)); // 复制一份给控制台
  }
  if (config_.output.toFile) {
    fileQue_->Push(std::move(msg)); // 直接移动给文件队列,提高性能
  }
}
#endif

} // namespace lyf
