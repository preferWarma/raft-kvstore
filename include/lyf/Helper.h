#pragma once
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <string>
#include <thread>

// 是否输出内部日志
// #define LYF_lyf_Internal_LOG

#ifdef LYF_lyf_Internal_LOG
#include "FastFormater.h"
#include <iostream>
#if _LIBCPP_STD_VER >= 20
#define lyf_Internal_LOG(fmt, ...)                                             \
  std::cout << FormatMessage<fmt>(__VA_ARGS__) << std::endl;
#else
#define lyf_Internal_LOG(fmt, ...)                                             \
  std::cout << FormatMessage(fmt, ##__VA_ARGS__) << std::endl;
#endif
#else
#define lyf_Internal_LOG(fmt, ...) ((void)0)
#endif

namespace lyf {

using std::string, std::thread;
namespace chrono = std::chrono;
using system_clock = chrono::system_clock;

/// @brief 获取当前时间戳
/// @return 当前时间戳
/// @note 单位为毫秒
inline int64_t GetCurrentTimeStamp() {
  return duration_cast<chrono::milliseconds>(
             system_clock::now().time_since_epoch())
      .count();
}

/// @brief 获取当前时间的格式化字符串
/// @param format 时间格式字符串, 默认为"%Y-%m-%d %H:%M:%S"
/// @return 格式化后的时间字符串
inline string GetCurrentTime(const string &format = "%Y-%m-%d %H:%M:%S") {
  auto now = system_clock::now();
  auto time_t = system_clock::to_time_t(now);
  std::stringstream ss;
  // 线程安全的时间格式化函数
  ss << std::put_time(std::localtime(&time_t), format.c_str());
  return ss.str();
}

/// @brief 格式化时间点为字符串
/// @param timePoint 时间点
/// @param format 时间格式字符串, 默认为"%Y-%m-%d %H:%M:%S"
/// @return 格式化后的时间字符串
inline string FormatTime(const system_clock::time_point &timePoint,
                         const string &format = "%Y-%m-%d %H:%M:%S") {
  std::time_t time = system_clock::to_time_t(timePoint);
  char buf[1024];
  strftime(buf, sizeof(buf), format.c_str(), localtime(&time));
  return buf;
}

/// @brief RAII 确保标志被重置
class FlagGuard {
public:
  FlagGuard(std::atomic<bool> &flag) : _flag(flag) {}
  ~FlagGuard() { _flag.store(false); }

private:
  std::atomic<bool> &_flag;
};

/// @brief 创建日志目录
/// @param path 日志文件路径
/// @return 创建成功返回true, 否则返回false
inline bool CreateLogDirectory(const string &path) {
  try {
    auto dir = std::filesystem::path(path).parent_path();
    if (!std::filesystem::exists(dir)) {
      std::filesystem::create_directories(dir);
    }
    return true;
  } catch (const std::exception &e) {
    lyf_Internal_LOG("Failed to create log directory: {}\n", e.what());
    return false;
  }
}

/// @brief 获取文件上一次修改时间
/// @param filePath 文件路径
/// @return 文件上一次修改时间
inline std::filesystem::file_time_type
getFileLastWriteTime(const string &filePath) {
  try {
    return std::filesystem::last_write_time(filePath);
  } catch (const std::exception &e) {
    lyf_Internal_LOG("Failed to get last write time: {}\n", e.what());
    return std::filesystem::file_time_type::min();
  }
}

inline bool StartWith(std::string_view str, std::string_view prefix) {
  if (str.size() < prefix.size()) {
    return false;
  }
  for (size_t i = 0; i < prefix.size(); ++i) {
    if (str[i] != prefix[i]) {
      return false;
    }
  }
  return true;
}

inline bool EndWith(std::string_view str, std::string_view suffix) {
  if (str.size() < suffix.size()) {
    return false;
  }
  for (size_t i = 0; i < suffix.size(); ++i) {
    if (str[str.size() - suffix.size() + i] != suffix[i]) {
      return false;
    }
  }
  return true;
}

} // namespace lyf
