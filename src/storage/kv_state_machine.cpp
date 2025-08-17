// src/storage/kv_state_machine.cpp
#include "storage/kv_state_machine.h"
#include "lyf/LogSystem.h"
#include <sstream>

namespace storage {
using std::lock_guard, std::mutex;

KVStateMachine::KVStateMachine(RocksDBStorage *storage) : storage_(storage) {
  if (!storage_) {
    LOG_FATAL("[KVStateMachine] Storage cannot be null");
    throw std::invalid_argument("Storage cannot be null");
  }
  
  // 初始化布隆过滤器
  bloom_filter_ = std::make_unique<BloomFilter>(BLOOM_FILTER_SIZE, BLOOM_FILTER_HASH_COUNT);
}

ApplyResult KVStateMachine::Apply(const std::string &command_data) {
  // 解析命令
  kvstore::KVCommand cmd;
  if (!cmd.ParseFromString(command_data)) {
    LOG_ERROR("[KVStateMachine] Failed to parse command: {}", command_data);
    return {false, "", "Failed to parse command"};
  }

  lock_guard<mutex> lock(mutex_);

  // 检查是否已经执行过（幂等性）
  ApplyResult cached_result;
  if (GetCachedResult(cmd.request_id(), &cached_result)) {
    return cached_result;
  }

  // 执行命令
  ApplyResult result;
  switch (cmd.type()) {
  case kvstore::KVCommand::PUT:
    result = ExecutePut(cmd);
    break;

  case kvstore::KVCommand::DELETE:
    result = ExecuteDelete(cmd);
    break;

  default:
    LOG_WARN("[KVStateMachine] Unknown command type: {}", cmd.type());
    result = {false, "", "Unknown command type"};
  }

  // 缓存结果
  if (!cmd.request_id().empty()) {
    CacheResult(cmd.request_id(), result);
  }

  return result;
}

bool KVStateMachine::Get(const std::string &key, std::string *value) {
  if (!value)
    return false;

  lock_guard<mutex> lock(mutex_);
  stats_.get_count++;

  return storage_->Get(key, value);
}

void KVStateMachine::AddToBloomFilter(const std::string& key) {
  lock_guard<mutex> lock(mutex_);
  bloom_filter_->Add(key);
}

bool KVStateMachine::MayExistInBloomFilter(const std::string& key) const {
  lock_guard<mutex> lock(mutex_);
  return bloom_filter_->MayExist(key);
}

ApplyResult KVStateMachine::ExecutePut(const kvstore::KVCommand &cmd) {
  if (cmd.key().empty()) {
    LOG_WARN("[KVStateMachine] Key cannot be empty");
    return {false, "", "Key cannot be empty"};
  }

  bool success = storage_->Put(cmd.key(), cmd.value());
  if (success) {
    stats_.put_count++;
    LOG_INFO("[StateMachine] PUT: {} = {}", cmd.key(), cmd.value());
    
    // 添加到布隆过滤器
    bloom_filter_->Add(cmd.key());
    
    return {true, "", ""};
  } else {
    LOG_WARN("[StateMachine] PUT: {} failed", cmd.key());
    return {false, "", "Failed to write to storage"};
  }
}

ApplyResult KVStateMachine::ExecuteDelete(const kvstore::KVCommand &cmd) {
  if (cmd.key().empty()) {
    LOG_WARN("[KVStateMachine] Key cannot be empty");
    return {false, "", "Key cannot be empty"};
  }

  bool success = storage_->Delete(cmd.key());
  if (success) {
    stats_.delete_count++;
    LOG_INFO("[StateMachine] DELETE: {}", cmd.key());
    return {true, "", ""};
  } else {
    LOG_WARN("[StateMachine] DELETE: {} failed", cmd.key());
    return {false, "", "Failed to delete from storage"};
  }
}

ApplyResult KVStateMachine::ExecuteGet(const kvstore::KVCommand &cmd) {
  if (cmd.key().empty()) {
    LOG_WARN("[KVStateMachine] Key cannot be empty");
    return {false, "", "Key cannot be empty"};
  }

  std::string value;
  bool found = storage_->Get(cmd.key(), &value);

  stats_.get_count++;

  if (found) {
    LOG_INFO("[StateMachine] GET: {} = {}", cmd.key(), value);
    return {true, value, ""};
  } else {
    LOG_WARN("[StateMachine] GET: {} not found", cmd.key());
    return {false, "", "Key not found"};
  }
}

std::string KVStateMachine::TakeSnapshot() {
  lock_guard<mutex> lock(mutex_);

  // 获取所有KV对
  std::map<std::string, std::string> kvs;
  if (!storage_->GetAllKVPairs(&kvs)) {
    LOG_WARN("[StateMachine] Failed to get all KV pairs");
    return "";
  }

  // 序列化为快照数据
  std::stringstream ss;
  ss << kvs.size() << "\n";

  for (const auto &[key, value] : kvs) {
    ss << key.size() << " " << key << " ";
    ss << value.size() << " " << value << "\n";
  }

  LOG_INFO("[StateMachine] Created snapshot with {} key-value pairs",
           kvs.size());

  return ss.str();
}

bool KVStateMachine::RestoreSnapshot(const std::string &snapshot_data) {
  lock_guard<mutex> lock(mutex_);

  std::stringstream ss(snapshot_data);
  size_t count;
  ss >> count;
  ss.ignore(); // 忽略换行符

  // 批量写入
  rocksdb::WriteBatch batch;

  for (size_t i = 0; i < count; i++) {
    size_t key_len, value_len;
    std::string key, value;

    ss >> key_len;
    ss.ignore(); // 忽略空格
    key.resize(key_len);
    ss.read(&key[0], key_len);
    ss.ignore(); // 忽略空格

    ss >> value_len;
    ss.ignore(); // 忽略空格
    value.resize(value_len);
    ss.read(&value[0], value_len);
    ss.ignore(); // 忽略换行符

    batch.Put(storage_->MakeKVKey(key), value);
    
    // 同时添加到布隆过滤器
    bloom_filter_->Add(key);
  }

  bool success = storage_->WriteBatch(&batch);

  if (success) {
    LOG_INFO("[StateMachine] Restored snapshot with {} key-value pairs", count);

    // 清空请求缓存
    request_cache_.clear();

    // 更新统计
    stats_.key_count = count;
  }

  return success;
}

size_t KVStateMachine::GetApproximateSize() {
  // 简单估算：每个KV对约100字节
  return stats_.key_count * 100;
}

KVStateMachine::Stats KVStateMachine::GetStats() const {
  lock_guard<mutex> lock(mutex_);
  return stats_;
}

void KVStateMachine::CacheResult(const std::string &request_id,
                                 const ApplyResult &result) {
  // 限制缓存大小
  if (request_cache_.size() >= MAX_CACHE_SIZE) {
    // 简单策略：清空一半
    auto it = request_cache_.begin();
    for (size_t i = 0; i < MAX_CACHE_SIZE / 2; i++) {
      it = request_cache_.erase(it);
    }
  }

  request_cache_[request_id] = result;
}

bool KVStateMachine::GetCachedResult(const std::string &request_id,
                                     ApplyResult *result) {
  auto it = request_cache_.find(request_id);
  if (it != request_cache_.end()) {
    *result = it->second;
    return true;
  }

  LOG_DEBUG("[StateMachine] Request {} not found in cache", request_id);
  return false;
}

} // namespace storage