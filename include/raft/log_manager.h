// include/raft/log_manager.h
#pragma once
#include "raft.pb.h"
#include <mutex>
#include <vector>

namespace raft {

// 日志管理器
class LogManager {
public:
  LogManager();
  ~LogManager() = default;

  // 添加日志条目
  void Append(const LogEntry &entry);
  void AppendEntries(uint64_t prev_log_index, uint64_t prev_log_term,
                     const std::vector<LogEntry> &entries);

  // 获取日志条目
  LogEntry GetEntry(uint64_t index) const;
  std::vector<LogEntry> GetEntries(uint64_t start_index,
                                   uint64_t end_index) const;

  // 日志截断
  void TruncateAfter(uint64_t index);

  // 获取日志信息
  uint64_t GetLastIndex() const;
  uint64_t GetLastTerm() const;
  uint64_t GetTermOfIndex(uint64_t index) const;
  size_t GetLogSize() const;

  // 一致性检查
  bool MatchLog(uint64_t index, uint64_t term) const;

  // 查找冲突
  std::pair<uint64_t, uint64_t> FindConflict(uint64_t prev_log_index,
                                             uint64_t prev_log_term) const;

  // 快照相关
  void InstallSnapshot(uint64_t last_included_index,
                       uint64_t last_included_term,
                       const std::vector<LogEntry> &entries);
  uint64_t GetSnapshotIndex() const { return snapshot_index_; }
  uint64_t GetSnapshotTerm() const { return snapshot_term_; }

  // 持久化支持
  void RestoreFromPersistentState(const std::vector<LogEntry> &entries);
  std::vector<LogEntry> GetAllEntries() const;

private:
  // 内部方法：获取实际的存储索引
  size_t ToStorageIndex(uint64_t logical_index) const;
  uint64_t ToLogicalIndex(size_t storage_index) const;

  // 无锁版本的内部方法（必须在持有锁的情况下调用）
  uint64_t GetLastIndexNoLock() const;
  uint64_t GetLastTermNoLock() const;
  uint64_t GetTermOfIndexNoLock(uint64_t index) const;

private:
  mutable std::mutex mutex_;

  // 日志存储
  std::vector<LogEntry> entries_;

  // 快照信息
  uint64_t snapshot_index_ = 0; // 快照中包含的最后日志索引
  uint64_t snapshot_term_ = 0;  // 快照中最后日志的任期

  // 缓存最后的索引和任期，避免重复计算
  mutable uint64_t cached_last_index_ = 0;
  mutable uint64_t cached_last_term_ = 0;
  mutable bool cache_valid_ = false;

  void InvalidateCache() { cache_valid_ = false; }
  void UpdateCache() const;
};

} // namespace raft