// src/storage/rocksdb_storage.cpp
#include "storage/rocksdb_storage.h"
#include "lyf/LogSystem.h"
#include <iomanip>
#include <sstream>

#define LOG_STATUS_IF_ONT_OK(op, status)                                       \
  if (!status.ok()) {                                                          \
    LOG_ERROR("rocksdb {} error: status is {}", op, status.ToString());        \
  }

namespace storage {

RocksDBStorage::RocksDBStorage(const std::string &db_path) : db_path_(db_path) {
  // 配置RocksDB选项
  options_.create_if_missing = true;
  options_.compression = rocksdb::kSnappyCompression;
  options_.write_buffer_size = 64 << 20; // 64MB
  options_.max_write_buffer_number = 3;
  options_.target_file_size_base = 64 << 20; // 64MB
  options_.max_background_compactions = 4;
  options_.max_background_flushes = 2;

  // 写选项
  write_options_.sync = true; // 确保持久化

  // 读选项
  read_options_.verify_checksums = true;
}

RocksDBStorage::~RocksDBStorage() { Close(); }

bool RocksDBStorage::Open() {
  rocksdb::Status status = rocksdb::DB::Open(options_, db_path_, &db_);
  if (!status.ok()) {
    LOG_FATAL("Failed to open RocksDB: {}", status.ToString());
    return false;
  }

  LOG_INFO("RocksDB opened at {}", db_path_);
  return true;
}

void RocksDBStorage::Close() {
  if (db_) {
    db_.reset();
    LOG_INFO("RocksDB closed");
  }
}

// KV操作
bool RocksDBStorage::Put(const std::string &key, const std::string &value) {
  if (!db_)
    return false;

  std::string db_key = MakeKVKey(key);
  rocksdb::Status status = db_->Put(write_options_, db_key, value);
  LOG_STATUS_IF_ONT_OK("Put", status);
  return status.ok();
}

bool RocksDBStorage::Get(const std::string &key, std::string *value) {
  if (!db_ || !value)
    return false;

  std::string db_key = MakeKVKey(key);
  rocksdb::Status status = db_->Get(read_options_, db_key, value);
  LOG_STATUS_IF_ONT_OK("Get", status);
  return status.ok();
}

bool RocksDBStorage::Delete(const std::string &key) {
  if (!db_)
    return false;

  std::string db_key = MakeKVKey(key);
  rocksdb::Status status = db_->Delete(write_options_, db_key);
  LOG_STATUS_IF_ONT_OK("Delete", status);
  return status.ok();
}

bool RocksDBStorage::WriteBatch(rocksdb::WriteBatch *batch) {
  if (!db_ || !batch)
    return false;

  rocksdb::Status status = db_->Write(write_options_, batch);
  LOG_STATUS_IF_ONT_OK("WriteBatch", status);
  return status.ok();
}

// Raft状态持久化
bool RocksDBStorage::SaveRaftState(uint64_t term,
                                   const std::string &voted_for) {
  if (!db_)
    return false;

  // 序列化Raft状态
  std::stringstream ss;
  ss << term << ":" << voted_for;

  rocksdb::Status status = db_->Put(write_options_, RAFT_STATE_KEY, ss.str());
  LOG_STATUS_IF_ONT_OK("SaveRaftState", status);
  return status.ok();
}

bool RocksDBStorage::LoadRaftState(uint64_t *term, std::string *voted_for) {
  if (!db_ || !term || !voted_for)
    return false;

  std::string value;
  rocksdb::Status status = db_->Get(read_options_, RAFT_STATE_KEY, &value);
  if (!status.ok()) {
    if (status.IsNotFound()) {
      // 初始状态
      *term = 0;
      voted_for->clear();
      return true;
    }
    LOG_STATUS_IF_ONT_OK("LoadRaftState", status);
    return false;
  }

  // 解析Raft状态
  size_t pos = value.find(':');
  if (pos == std::string::npos) {
    LOG_ERROR("Invalid Raft state format: {}", value);
    return false;
  }

  *term = std::stoull(value.substr(0, pos));
  *voted_for = value.substr(pos + 1);

  return true;
}

// 日志持久化
bool RocksDBStorage::AppendLog(const raft::LogEntry &entry) {
  if (!db_) {
    LOG_ERROR("RocksDB not opened");
    return false;
  }

  std::string key = MakeLogKey(entry.index());
  std::string value;
  if (!entry.SerializeToString(&value)) {
    LOG_ERROR("Failed to serialize log entry");
    return false;
  }

  rocksdb::Status status = db_->Put(write_options_, key, value);
  LOG_STATUS_IF_ONT_OK("AppendLog", status);
  return status.ok();
}

bool RocksDBStorage::GetLog(uint64_t index, raft::LogEntry *entry) {
  if (!db_ || !entry) {
    LOG_ERROR("RocksDB not opened or entry is null");
    return false;
  }

  std::string key = MakeLogKey(index);
  std::string value;

  rocksdb::Status status = db_->Get(read_options_, key, &value);
  if (!status.ok()) {
    LOG_STATUS_IF_ONT_OK("GetLog", status);
    return false;
  }

  return entry->ParseFromString(value);
}

bool RocksDBStorage::GetLogs(uint64_t start_index, uint64_t end_index,
                             std::vector<raft::LogEntry> *entries) {
  if (!db_ || !entries || start_index > end_index) {
    LOG_ERROR("RocksDB not opened or entry is null or Invalid log index range");
    return false;
  }

  entries->clear();

  // 使用迭代器批量读取
  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(read_options_));

  std::string start_key = MakeLogKey(start_index);
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    // 检查是否是日志键
    if (!iter->key().starts_with(RAFT_LOG_PREFIX)) {
      break;
    }

    // 解析索引
    uint64_t index;
    if (!ParseLogKey(iter->key().ToString(), &index)) {
      continue;
    }

    if (index >= end_index) {
      break;
    }

    // 解析日志条目
    raft::LogEntry entry;
    if (entry.ParseFromString(iter->value().ToString())) {
      entries->push_back(std::move(entry));
    }
  }

  return true;
}

bool RocksDBStorage::DeleteLogsAfter(uint64_t index) {
  if (!db_) {
    LOG_ERROR("RocksDB not opened");
    return false;
  }

  rocksdb::WriteBatch batch;
  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(read_options_));

  std::string start_key = MakeLogKey(index + 1);
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(RAFT_LOG_PREFIX)) {
      break;
    }

    batch.Delete(iter->key());
  }

  rocksdb::Status status = db_->Write(write_options_, &batch);
  LOG_STATUS_IF_ONT_OK("DeleteLogsAfter", status);
  return status.ok();
}

bool RocksDBStorage::GetLastLogIndex(uint64_t *index) {
  if (!db_ || !index) {
    LOG_ERROR("RocksDB not opened or index is null");
    return false;
  }

  *index = 0;

  // 反向迭代找到最后一个日志条目
  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(read_options_));

  // 从日志键的最大可能值开始反向查找
  std::string max_log_key = RAFT_LOG_PREFIX;
  max_log_key.append(20, '\xFF'); // 添加足够的0xFF使其成为最大键

  iter->SeekForPrev(max_log_key);

  while (iter->Valid()) {
    if (iter->key().starts_with(RAFT_LOG_PREFIX)) {
      uint64_t log_index;
      if (ParseLogKey(iter->key().ToString(), &log_index)) {
        *index = log_index;
        return true;
      }
    }
    iter->Prev();
  }

  return true; // 没有日志也是正常的
}

// 快照相关
bool RocksDBStorage::SaveSnapshot(uint64_t index, uint64_t term,
                                  const std::string &state_machine_data) {
  if (!db_) {
    LOG_ERROR("RocksDB not opened");
    return false;
  }

  rocksdb::WriteBatch batch;

  // 保存快照元数据
  std::stringstream meta;
  meta << index << ":" << term;
  batch.Put(SNAPSHOT_META_KEY, meta.str());

  // 保存状态机数据
  batch.Put(SNAPSHOT_DATA_KEY, state_machine_data);

  // 删除快照之前的日志
  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(read_options_));
  for (iter->Seek(RAFT_LOG_PREFIX); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(RAFT_LOG_PREFIX)) {
      break;
    }

    uint64_t log_index;
    if (ParseLogKey(iter->key().ToString(), &log_index) && log_index <= index) {
      batch.Delete(iter->key());
    }
  }

  rocksdb::Status status = db_->Write(write_options_, &batch);
  LOG_STATUS_IF_ONT_OK("SaveSnapshot", status);
  return status.ok();
}

bool RocksDBStorage::LoadSnapshot(uint64_t *index, uint64_t *term,
                                  std::string *state_machine_data) {
  if (!db_ || !index || !term || !state_machine_data) {
    LOG_ERROR(
        "RocksDB not opened or index, term, or state_machine_data is null");
    return false;
  }

  // 加载快照元数据
  std::string meta;
  rocksdb::Status status = db_->Get(read_options_, SNAPSHOT_META_KEY, &meta);
  if (!status.ok()) {
    return false;
  }

  // 解析元数据
  size_t pos = meta.find(':');
  if (pos == std::string::npos) {
    LOG_ERROR("Invalid snapshot meta data: {}", meta);
    return false;
  }

  *index = std::stoull(meta.substr(0, pos));
  *term = std::stoull(meta.substr(pos + 1));

  // 加载状态机数据
  status = db_->Get(read_options_, SNAPSHOT_DATA_KEY, state_machine_data);
  LOG_STATUS_IF_ONT_OK("LoadSnapshot", status);
  return status.ok();
}

bool RocksDBStorage::HasSnapshot() {
  if (!db_) {
    LOG_ERROR("RocksDB not opened");
    return false;
  }

  std::string value;
  rocksdb::Status status = db_->Get(read_options_, SNAPSHOT_META_KEY, &value);
  LOG_STATUS_IF_ONT_OK("HasSnapshot", status);
  return status.ok();
}

bool RocksDBStorage::GetAllKVPairs(std::map<std::string, std::string> *kvs) {
  if (!db_ || !kvs) {
    LOG_ERROR("RocksDB not opened or kvs is null");
    return false;
  }

  kvs->clear();

  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(read_options_));
  for (iter->Seek(KV_PREFIX); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(KV_PREFIX)) {
      break;
    }

    // 去掉前缀得到实际的key
    std::string key = iter->key().ToString().substr(strlen(KV_PREFIX));
    kvs->emplace(key, iter->value().ToString());
  }

  return true;
}

// 辅助方法
std::string RocksDBStorage::MakeLogKey(uint64_t index) const {
  std::stringstream ss;
  ss << RAFT_LOG_PREFIX << std::setfill('0') << std::setw(20) << index;
  return ss.str();
}

std::string RocksDBStorage::MakeKVKey(const std::string &key) const {
  return std::string(KV_PREFIX) + key;
}

bool RocksDBStorage::ParseLogKey(const std::string &key,
                                 uint64_t *index) const {
  if (!rocksdb::Slice(key).starts_with(RAFT_LOG_PREFIX)) {
    return false;
  }

  try {
    *index = std::stoull(key.substr(strlen(RAFT_LOG_PREFIX)));
    return true;
  } catch (...) {
    return false;
  }
}

std::string RocksDBStorage::GetStats() const {
  if (!db_) {
    LOG_ERROR("RocksDB not opened");
    return "Database not open";
  }

  std::string stats;
  db_->GetProperty("rocksdb.stats", &stats);
  return stats;
}

} // namespace storage