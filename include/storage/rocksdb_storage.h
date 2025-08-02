// include/storage/rocksdb_storage.h
#pragma once

#include "raft.pb.h"
#include <map>
#include <memory>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#include <string>
#include <vector>

namespace storage {
using std::string, std::vector, std::map;

class RocksDBStorage {
public:
  explicit RocksDBStorage(const string &db_path);
  ~RocksDBStorage();

  // 初始化数据库
  bool Open();
  void Close();

  // KV操作
  bool Put(const string &key, const string &value);
  bool Get(const string &key, string *value);
  bool Delete(const string &key);

  // 批量操作
  bool WriteBatch(rocksdb::WriteBatch *batch);

  // Raft状态持久化
  bool SaveRaftState(uint64_t term, const string &voted_for);
  bool LoadRaftState(uint64_t *term, string *voted_for);

  // 日志持久化
  bool AppendLog(const raft::LogEntry &entry);
  bool GetLog(uint64_t index, raft::LogEntry *entry);
  bool GetLogs(uint64_t start_index, uint64_t end_index,
               vector<raft::LogEntry> *entries);
  bool DeleteLogsAfter(uint64_t index);
  bool GetLastLogIndex(uint64_t *index);

  // 快照相关
  bool SaveSnapshot(uint64_t index, uint64_t term,
                    const string &state_machine_data);
  bool LoadSnapshot(uint64_t *index, uint64_t *term,
                    string *state_machine_data);
  bool HasSnapshot();

  // 获取所有KV对（用于快照）
  bool GetAllKVPairs(std::map<string, string> *kvs);

  // 性能统计
  string GetStats() const;

private:
  // 列族名称
  static constexpr const char *DEFAULT_CF = "default";
  static constexpr const char *KV_CF = "kv";
  static constexpr const char *RAFT_LOG_CF = "raft_log";
  static constexpr const char *RAFT_STATE_CF = "raft_state";
  static constexpr const char *SNAPSHOT_CF = "snapshot";

  // 辅助方法
public:
  string MakeLogKey(uint64_t index) const;
  string MakeKVKey(const string &key) const;
  bool ParseLogKey(const string &key, uint64_t *index) const;

  string db_path_;
  std::unique_ptr<rocksdb::DB> db_;
  rocksdb::Options options_;
  rocksdb::WriteOptions write_options_;
  rocksdb::ReadOptions read_options_;

  // 列族句柄
  map<string, rocksdb::ColumnFamilyHandle *> cf_handles_;

  // 初始化列族
  bool InitColumnFamilies();
};

} // namespace storage