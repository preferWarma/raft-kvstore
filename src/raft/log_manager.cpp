// src/raft/log_manager.cpp
#include "raft/log_manager.h"
#include "lyf/LogSystem.h"
#include <algorithm>
#include <cassert>
#include <mutex>

using std::lock_guard, std::mutex;
using std::vector, std::string;

namespace raft {

LogManager::LogManager() {
  // 添加一个哨兵条目，简化边界处理
  // 哨兵条目索引为0，任期为0，类型为NOOP
  // 所有日志条目索引都从1开始
  LogEntry dummy;
  dummy.set_index(0);
  dummy.set_term(0);
  dummy.set_type(LogEntry::NOOP);
  entries_.push_back(std::move(dummy));
}

void LogManager::Append(const LogEntry &entry) {
  lock_guard<mutex> lock(mutex_);
  // 使用无锁版本避免死锁
  assert(entry.index() == GetLastIndexNoLock() + 1);
  entries_.push_back(entry);

  // 添加新日志条目后，日志的最后索引和任期会发生变化。此时调用
  // InvalidateCache() 会将 cache_valid_ 标志设为false，使缓存失效。之后在调用
  // GetLastIndexNoLock() 或 GetLastTermNoLock() 时，会触发 UpdateCache()
  // 重新计算并更新缓存值，确保后续获取的日志信息是最新的。
  InvalidateCache();

  LOG_DEBUG("LogManager: Appended entry index={}, term={}", entry.index(),
            entry.term());
}

void LogManager::AppendEntries(uint64_t prev_log_index, uint64_t prev_log_term,
                               const std::vector<LogEntry> &entries) {
  lock_guard<mutex> lock(mutex_);

  // 检查prev_log的一致性（使用无锁版本）
  if (prev_log_index > 0 &&
      GetTermOfIndexNoLock(prev_log_index) != prev_log_term) {
    LOG_DEBUG("LogManager: Log mismatch at index={}, expected term={}",
              prev_log_index, prev_log_term);
    return;
  }

  // 找到插入点
  size_t insert_idx = ToStorageIndex(prev_log_index + 1);

  // 检查是否有冲突
  // 不同任期号表明日志存在分歧，需要以领导者日志为准
  size_t i = 0;
  for (; i < entries.size() && insert_idx + i < entries_.size(); ++i) {
    if (entries_[insert_idx + i].term() != entries[i].term()) {
      // 发现冲突，删除这个位置及之后的所有条目
      LOG_DEBUG("LogManager: Conflict at index={}, truncating",
                ToLogicalIndex(insert_idx + i));

      entries_.erase(entries_.begin() + insert_idx + i, entries_.end());
      break;
    }
  }

  // 追加新条目
  for (; i < entries.size(); ++i) {
    entries_.push_back(entries[i]);
  }

  // 同Append, 追加后使缓存失效
  InvalidateCache();
}

LogEntry LogManager::GetEntry(uint64_t index) const {
  lock_guard<mutex> lock(mutex_);

  if (index <= snapshot_index_) {
    // 请求的日志在快照中
    LogEntry entry;
    entry.set_index(index);
    entry.set_term(snapshot_term_);
    return entry;
  }

  size_t storage_idx = ToStorageIndex(index);
  if (storage_idx >= entries_.size()) {
    // 索引越界
    LOG_WARN("LogManager: Index out of range, index={}", index);
    return LogEntry();
  }

  return entries_[storage_idx];
}

vector<LogEntry> LogManager::GetEntries(uint64_t start_index,
                                        uint64_t end_index) const {
  lock_guard<mutex> lock(mutex_);

  vector<LogEntry> result;

  // 调整范围
  start_index = std::max(start_index, snapshot_index_ + 1);
  end_index = std::min(end_index, GetLastIndexNoLock() + 1);

  if (start_index >= end_index) {
    return result;
  }

  size_t start_storage = ToStorageIndex(start_index);
  size_t end_storage = ToStorageIndex(end_index);

  result.reserve(end_storage - start_storage);
  for (size_t i = start_storage; i < end_storage && i < entries_.size(); ++i) {
    result.push_back(entries_[i]);
  }

  return result;
}

void LogManager::TruncateAfter(uint64_t index) {
  lock_guard<mutex> lock(mutex_);

  if (index <= snapshot_index_) {
    // 不能截断快照中的日志
    return;
  }

  size_t storage_idx = ToStorageIndex(index + 1);
  if (storage_idx < entries_.size()) {
    entries_.erase(entries_.begin() + storage_idx, entries_.end());
    InvalidateCache();

    LOG_DEBUG("LogManager: Truncated after index={}", index);
  }
}

uint64_t LogManager::GetLastIndex() const {
  lock_guard<mutex> lock(mutex_);
  return GetLastIndexNoLock();
}

uint64_t LogManager::GetLastTerm() const {
  lock_guard<mutex> lock(mutex_);
  return GetLastTermNoLock();
}

uint64_t LogManager::GetTermOfIndex(uint64_t index) const {
  lock_guard<mutex> lock(mutex_);
  return GetTermOfIndexNoLock(index);
}

// 无锁版本的内部方法实现
uint64_t LogManager::GetLastIndexNoLock() const {
  UpdateCache();
  return cached_last_index_;
}

uint64_t LogManager::GetLastTermNoLock() const {
  UpdateCache();
  return cached_last_term_;
}

uint64_t LogManager::GetTermOfIndexNoLock(uint64_t index) const {
  if (index == 0) {
    return 0;
  }

  if (index == snapshot_index_) {
    return snapshot_term_;
  }

  if (index < snapshot_index_) {
    // 无法获取快照之前的日志任期
    return 0;
  }

  size_t storage_idx = ToStorageIndex(index);
  if (storage_idx >= entries_.size()) { // 超出范围
    return 0;
  }

  return entries_[storage_idx].term();
}

size_t LogManager::GetLogSize() const {
  lock_guard<mutex> lock(mutex_);
  return snapshot_index_ + entries_.size();
}

bool LogManager::MatchLog(uint64_t index, uint64_t term) const {
  return GetTermOfIndex(index) == term;
}

std::pair<uint64_t, uint64_t>
LogManager::FindConflict(uint64_t prev_log_index,
                         uint64_t prev_log_term) const {
  lock_guard<mutex> lock(mutex_);

  // 如果prev_log_index在快照中，无法检查
  if (prev_log_index <= snapshot_index_) {
    return {snapshot_index_ + 1, 0};
  }

  // 检查是否超出范围
  if (prev_log_index > GetLastIndexNoLock()) {
    return {GetLastIndexNoLock() + 1, 0};
  }

  // 获取该位置的任期
  uint64_t actual_term = GetTermOfIndexNoLock(prev_log_index);
  if (actual_term != prev_log_term) {
    // 找到该任期的第一个条目
    uint64_t conflict_index = prev_log_index;
    while (conflict_index > snapshot_index_ + 1 &&
           GetTermOfIndexNoLock(conflict_index - 1) == actual_term) {
      conflict_index--;
    }
    return {conflict_index, actual_term};
  }

  return {0, 0}; // 没有冲突
}

void LogManager::InstallSnapshot(uint64_t last_included_index,
                                 uint64_t last_included_term,
                                 const vector<LogEntry> &entries) {
  lock_guard<mutex> lock(mutex_);

  snapshot_index_ = last_included_index;
  snapshot_term_ = last_included_term;

  // 清空现有日志
  entries_.clear();

  // 添加哨兵条目
  LogEntry dummy;
  dummy.set_index(snapshot_index_);
  dummy.set_term(snapshot_term_);
  dummy.set_type(LogEntry::NOOP);
  entries_.push_back(std::move(dummy));

  // 添加快照之后的条目
  for (const auto &entry : entries) {
    if (entry.index() > snapshot_index_) {
      entries_.push_back(entry);
    }
  }

  InvalidateCache();
}

void LogManager::RestoreFromPersistentState(const vector<LogEntry> &entries) {
  lock_guard<mutex> lock(mutex_);

  entries_.clear();

  // 添加哨兵条目
  if (entries.empty() || entries[0].index() != 0) {
    LogEntry dummy;
    dummy.set_index(0);
    dummy.set_term(0);
    dummy.set_type(LogEntry::NOOP);
    entries_.push_back(std::move(dummy));
  }

  // 复制日志条目
  for (const auto &entry : entries) {
    if (entry.index() > 0) {
      entries_.push_back(entry);
    }
  }

  InvalidateCache();
}

vector<LogEntry> LogManager::GetAllEntries() const {
  lock_guard<mutex> lock(mutex_);
  return entries_;
}

// 将外部使用的『逻辑日志索引』转换为本地数组 entries_ 的存储索引
// 逻辑日志索引 = 快照索引 + 存储索引
size_t LogManager::ToStorageIndex(uint64_t logical_index) const {
  if (logical_index <= snapshot_index_) {
    return 0;
  }
  return logical_index - snapshot_index_;
}

// 将本地数组 entries_ 的存储索引转换为对外暴露的『逻辑日志索引』。
// 逻辑日志索引 = 快照索引 + 存储索引
uint64_t LogManager::ToLogicalIndex(size_t storage_index) const {
  return snapshot_index_ + storage_index;
}

void LogManager::UpdateCache() const {
  if (cache_valid_) {
    return;
  }

  if (entries_.empty()) {
    cached_last_index_ = snapshot_index_;
    cached_last_term_ = snapshot_term_;
  } else {
    cached_last_index_ = entries_.back().index();
    cached_last_term_ = entries_.back().term();
  }

  cache_valid_ = true;
}

} // namespace raft