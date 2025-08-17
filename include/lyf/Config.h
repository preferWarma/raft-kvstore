#pragma once
#include "Enum.h"
#include "Helper.h"
#include "JsonHelper.h"
#include "Singleton.h"
#include "Timer.h"
#include <filesystem>
#include <string>

// 是否编译云存储模块
// #define CLOUD_INCLUDE

namespace lyf {
using std::string;
using std::chrono::milliseconds;

struct Config : Singleton<Config> {
  friend class Singleton<Config>;

private:
  // 用于定时检测配置文件执行热更新
  Timer timer_;                                   // 异步定时器
  std::filesystem::file_time_type lastWriteTime_; // 上次配置文件修改时间

public:
  struct BasicConfig {
    size_t maxQueueSize; // 日志缓存队列初始时最大长度限制(0表示无限制)
    QueueFullPolicy queueFullPolicy; // 队列满时的处理策略(默认:BLOCK)
    size_t maxBlockTime_us; // 队列满时主线程阻塞最大时间，超过则会自动扩容
    size_t maxDropCount; // 队列满时丢弃最大数量，超过则会自动扩容
    size_t autoExpandMultiply; // 最大自动扩容倍数（以初始的最大队列长度为基准）
  } basic;

  struct OutPutConfig {
    string logRootDir; // 日志根目录
    bool toFile;       // 是否输出到文件
    bool toConsole;    // 是否输出到控制台
    int minLogLevel; // 最小日志级别(0:DEBUG,1:INFO,2:WARNING,3:ERROR,4:FATAL)
  } output;

  struct PerformanceConfig {
    bool enableAsyncConsole;           // 是否异步控制台输出
    size_t consoleBatchSize;           // 控制台小批次
    size_t fileBatchSize;              // 文件大批次
    milliseconds consoleFlushInterval; // 控制台刷新间隔
    milliseconds fileFlushInterval;    // 文件刷新间隔
    size_t consoleBufferSize_kb;       // 控制台预分配缓冲区大小
    size_t fileBufferSize_kb;          // 文件预分配缓冲区大小
  } performance;

  struct RotationConfig {
    size_t maxFileSize;  // 最大文件大小
    size_t maxFileCount; // 保留最近文件数量
  } rotation;

  struct CloudConfig {
    bool enable;            // 是否启用云存储
    string serverUrl;       // 云存储服务器URL
    string uploadEndpoint;  // 上传接口路径
    int uploadTimeout_s;    // 上传超时时间（秒）
    bool deleteAfterUpload; // 上传后是否删除本地文件
    string apiKey;          // API密钥
    int maxRetries;         // 最大重试次数
    int retryDelay_s;       // 重试延迟时间（秒）
    int maxQueueSize;       // 上传队列最大大小
  } cloud;

  void Init() {
    auto &helper = JsonHelper::GetInstance();
    { // basic config
      basic.maxQueueSize = helper.Get<int>("basic.maxQueueSize", 0);
      basic.queueFullPolicy =
          helper.Get<string>("basic.queueFullPolicy", "BLOCK") == "BLOCK"
              ? QueueFullPolicy::BLOCK
              : QueueFullPolicy::DROP;
      basic.maxBlockTime_us = helper.Get<int>("basic.maxBlockTime_us", 16);
      basic.maxDropCount = helper.Get<int>("basic.maxDropCount", 40960);
      basic.autoExpandMultiply = helper.Get<int>("basic.autoExpandMultiply", 4);
    }

    { // output config
      output.logRootDir = helper.Get<string>("output.logRootDir", "./log");
      output.toFile = helper.Get<bool>("output.toFile", true);
      output.toConsole = helper.Get<bool>("output.toConsole", false);
      output.minLogLevel = helper.Get<int>("output.minLogLevel", 0);
    }

    { // performance config
      performance.consoleBatchSize =
          helper.Get<int>("performance.consoleBatchSize", 512);
      performance.fileBatchSize =
          helper.Get<int>("performance.fileBatchSize", 1024);
      performance.consoleFlushInterval = milliseconds(
          helper.Get<int>("performance.consoleFlushInterval_ms", 50));
      performance.fileFlushInterval = milliseconds(
          helper.Get<int>("performance.fileFlushInterval_ms", 100));
      performance.enableAsyncConsole =
          helper.Get<bool>("performance.enableAsyncConsole", true);
      performance.consoleBufferSize_kb =
          helper.Get<int>("performance.consoleBufferSize_kb", 16);
      performance.fileBufferSize_kb =
          helper.Get<int>("performance.fileBufferSize_kb", 32);
    }

    { // rotation config
      rotation.maxFileSize = helper.Get<int>("rotation.maxLogFileSize",
                                             1024 * 1024 * 10); // 默认10MB
      rotation.maxFileCount = helper.Get<int>("rotation.maxLogFileCount", 10);
    }

    { // cloud config
      cloud.enable = helper.Get<bool>("cloud.enable", false);
      cloud.serverUrl = helper.Get<string>("cloud.serverUrl", "");
      cloud.uploadEndpoint = helper.Get<string>("cloud.uploadEndpoint", "");
      cloud.uploadTimeout_s = helper.Get<int>("cloud.uploadTimeout_s", 30);
      cloud.deleteAfterUpload =
          helper.Get<bool>("cloud.deleteAfterUpload", false);
      cloud.apiKey = helper.Get<string>("cloud.apiKey", "");
      cloud.maxRetries = helper.Get<int>("cloud.maxRetries", 3);
      cloud.retryDelay_s = helper.Get<int>("cloud.retryDelay_s", 5);
      cloud.maxQueueSize = helper.Get<int>("cloud.maxQueueSize", 100);
    }
  }

protected:
  Config() {
    Init();
    StartListen();
  }

private:
  void StartListen() {
    auto &helper = JsonHelper::GetInstance();
    auto &configPath = helper.getFilePath();
    if (configPath.empty()) {
      return;
    }
    lastWriteTime_ = getFileLastWriteTime(configPath);

    timer_.setInterval(1000, [&]() {
      auto newLastWriteTime = getFileLastWriteTime(configPath);
      if (newLastWriteTime > lastWriteTime_) {
        lyf_Internal_LOG("Config file '{}' has been modified, reloading.",
                         configPath);
        helper.LoadFromFile(); // 重新加载配置文件
        Init();                // 重新初始化配置
        lastWriteTime_ = newLastWriteTime;
      }
    });
  }
};
} // namespace lyf