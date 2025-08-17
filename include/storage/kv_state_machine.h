// include/storage/kv_state_machine.h
#pragma once

#include "kvstore.pb.h"
#include "rocksdb_storage.h"
#include "bloom_filter.h"
#include <mutex>
#include <string>
#include <unordered_map>

namespace storage {

// 应用到状态机的命令结果
struct ApplyResult {
  bool success = false;
  std::string value; // 用于Get操作
  std::string error;
};

class KVStateMachine {
public:
  explicit KVStateMachine(RocksDBStorage *storage);
  ~KVStateMachine() = default;

  // 布隆过滤器操作
  void AddToBloomFilter(const std::string& key);
  bool MayExistInBloomFilter(const std::string& key) const;

  // 应用命令到状态机
  ApplyResult Apply(const std::string &command_data);

  // 直接KV操作（用于只读操作）
  bool Get(const std::string &key, std::string *value);

  // 创建快照
  std::string TakeSnapshot();

  // 从快照恢复
  bool RestoreSnapshot(const std::string &snapshot_data);

  // 获取状态机大小（用于决定何时创建快照）
  size_t GetApproximateSize();

  // 统计信息
  struct Stats {
    uint64_t put_count = 0;
    uint64_t get_count = 0;
    uint64_t delete_count = 0;
    uint64_t key_count = 0;
  };

  Stats GetStats() const;

private:
  // 执行具体的KV操作
  ApplyResult ExecutePut(const kvstore::KVCommand &cmd);
  ApplyResult ExecuteDelete(const kvstore::KVCommand &cmd);
  ApplyResult ExecuteGet(const kvstore::KVCommand &cmd);

  RocksDBStorage *storage_; // 不拥有所有权
  mutable std::mutex mutex_;

  // 统计信息
  mutable Stats stats_;

  // 请求去重（防止重复执行）
  std::unordered_map<std::string, ApplyResult> request_cache_;
  static constexpr size_t MAX_CACHE_SIZE = 10000;

  // 布隆过滤器
  std::unique_ptr<BloomFilter> bloom_filter_;
  static constexpr size_t BLOOM_FILTER_SIZE = 1000000; // 100万个位
  static constexpr size_t BLOOM_FILTER_HASH_COUNT = 3;  // 3个哈希函数

  void CacheResult(const std::string &request_id, const ApplyResult &result);
  bool GetCachedResult(const std::string &request_id, ApplyResult *result);
};

} // namespace storage