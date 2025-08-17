#pragma once

#include <string>

namespace lyf {

enum class RotationType {
  BY_SIZE,         // 按文件大小轮转
  BY_TIME,         // 按时间轮转(每天)
  BY_SIZE_AND_TIME // 按大小和时间轮转
};

enum class LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
  FATAL = 4,
};

enum class QueueFullPolicy {
  BLOCK = 0, // 队列满时阻塞
  DROP = 1   // 队列满时丢弃
};

inline std::string LevelToString(LogLevel level) {
  switch (level) {
  case LogLevel::DEBUG:
    return "DEBUG";
  case LogLevel::INFO:
    return "INFO ";
  case LogLevel::WARN:
    return "WARN ";
  case LogLevel::ERROR:
    return "ERROR";
  case LogLevel::FATAL:
    return "FATAL";
  default:
    return "UNKNOWN";
  }
}

inline std::string LevelColor(LogLevel level) {
  switch (level) {
  case LogLevel::DEBUG:
    return "\033[0;37m"; // 白色
  case LogLevel::INFO:
    return "\033[0;32m"; // 绿色
  case LogLevel::WARN:
    return "\033[1;33m"; // 黄色
  case LogLevel::ERROR:
    return "\033[1;31m"; // 红色
  case LogLevel::FATAL:
    return "\033[1;35m"; // 紫色
  default:
    return "\033[0m"; // 默认
  }
}

} // namespace lyf