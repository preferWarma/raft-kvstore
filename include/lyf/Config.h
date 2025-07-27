#pragma once
#include <cstdlib>
#include <string>

#define getEnv(var, default)                                                   \
  (std::getenv(var) == nullptr ? default : std::getenv(var))

namespace lyf {

const std::string LOG_FILE_PATH = getEnv("LOG_FILE_PATH", "log/log.txt");

const std::string LOG_MODE = getEnv("LOG_MODE", "CONSOLE | FILE");

/*
    日志级别:
    0: DEBUG: 调试信息
    1: INFO: 一般信息
    2: WARNING: 警告信息
    3: ERROR: 错误信息
    4: FATAL: 致命错误信息
*/
const int MIN_LOG_LEVEL = std::stoi(getEnv("MIN_LOG_LEVEL", "1"));

} // namespace lyf
