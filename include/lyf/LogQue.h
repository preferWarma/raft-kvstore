#pragma once

#include "Config.h"
#include "Enum.h"
#include "third/concurrentqueue.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <string>
#include <thread>
#include <vector>

namespace lyf {
using time_point = std::chrono::system_clock::time_point;
using std::string, std::vector, std::atomic;
using std::chrono::system_clock;

struct LogMessage {
  LogLevel level;
  string content;
  time_point time;

  LogMessage(LogLevel level, string content)
      : level(level), content(std::move(content)), time(system_clock::now()) {}

  LogMessage(LogMessage &&other) noexcept = default;
  LogMessage &operator=(LogMessage &&other) noexcept = default;

  LogMessage(const LogMessage &other) = default;
  LogMessage &operator=(const LogMessage &other) = default;
};

class LogQueue {
  using milliseconds = std::chrono::milliseconds;
  using ConcurrentQueue = moodycamel::ConcurrentQueue<LogMessage>;

public:
  using QueueType = std::deque<LogMessage>;

public:
  LogQueue(size_t maxSize)
      : queue_(maxSize), config_(Config::GetInstance()), dropCount_(0),
        currentMaxQueueSize_(config_.basic.maxQueueSize) {}

  void Push(LogMessage &&msg) noexcept { PushInternal(std::move(msg)); }

  void Push(const LogMessage &msg) noexcept { PushInternal(msg); }

  size_t PopBatch(QueueType &output, size_t batch_size = 1024) {
    return queue_.try_dequeue_bulk(std::back_inserter(output), batch_size);
  }

  void Stop() {}

  size_t size_approx() const { return queue_.size_approx(); }

private:
  template <typename T> void PushInternal(T &&msg) noexcept {
    if (config_.basic.maxQueueSize == 0 ||
        queue_.size_approx() <
            currentMaxQueueSize_.load(std::memory_order_relaxed)) {
      queue_.enqueue(std::forward<T>(msg));
    } else {
      if (config_.basic.queueFullPolicy == QueueFullPolicy::BLOCK) {
        size_t sleepTime_us = 1;
        size_t total_sleepTime_us = 0;
        while (!queue_.try_enqueue(std::forward<T>(msg))) {
          std::this_thread::sleep_for(std::chrono::microseconds(sleepTime_us));
          total_sleepTime_us += sleepTime_us;
          sleepTime_us =
              std::min(sleepTime_us * 2, config_.basic.maxBlockTime_us);
          if (total_sleepTime_us > config_.basic.maxBlockTime_us &&
              currentMaxQueueSize_.load(std::memory_order_relaxed) <
                  config_.basic.maxQueueSize *
                      config_.basic.autoExpandMultiply) {
            // 超过可容忍的最大阻塞时间且未到硬性上限，允许扩容
            queue_.enqueue(std::forward<T>(msg));
            currentMaxQueueSize_.fetch_add(1, std::memory_order_relaxed);
            return;
          }
        }
      } else if (config_.basic.queueFullPolicy == QueueFullPolicy::DROP) {
        dropCount_++;
        if (dropCount_ > config_.basic.maxDropCount &&
            currentMaxQueueSize_.load(std::memory_order_relaxed) <
                config_.basic.maxQueueSize * config_.basic.autoExpandMultiply) {
          // 超过可容忍的最大丢弃次数且未到硬性上限，允许扩容
          queue_.enqueue(std::forward<T>(msg));
          currentMaxQueueSize_.fetch_add(1, std::memory_order_relaxed);
          dropCount_.store(0, std::memory_order_relaxed);
          return;
        }
      }
    }
  }

private:
  ConcurrentQueue queue_;
  lyf::Config &config_;
  std::atomic<size_t> dropCount_;
  std::atomic<size_t> currentMaxQueueSize_;
}; // class LogQueue

} // namespace lyf
