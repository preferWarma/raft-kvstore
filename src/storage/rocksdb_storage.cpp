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

RocksDBStorage::~RocksDBStorage() {
  // 清理列族句柄
  for (auto &[name, handle] : cf_handles_) {
    if (handle) {
      db_->DestroyColumnFamilyHandle(handle);
    }
  }
  cf_handles_.clear();
  Close();
}

bool RocksDBStorage::InitColumnFamilies() {
  // 清理现有句柄
  cf_handles_.clear();
  if (db_) {
    db_.reset();
  }

  // 列族描述符
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

  // 始终包含默认列族
  rocksdb::ColumnFamilyOptions default_options;
  default_options.compression = rocksdb::kSnappyCompression;
  column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, default_options);

  // KV列族配置
  rocksdb::ColumnFamilyOptions kv_options;
  kv_options.compression = rocksdb::kSnappyCompression;
  kv_options.write_buffer_size = 64 << 20;
  kv_options.max_write_buffer_number = 3;
  column_families.emplace_back(KV_CF, kv_options);

  // Raft日志列族配置
  rocksdb::ColumnFamilyOptions log_options;
  log_options.compression = rocksdb::kSnappyCompression;
  log_options.write_buffer_size = 32 << 20;
  log_options.max_write_buffer_number = 2;
  column_families.emplace_back(RAFT_LOG_CF, log_options);

  // Raft状态列族配置
  rocksdb::ColumnFamilyOptions state_options;
  state_options.compression = rocksdb::kSnappyCompression;
  state_options.write_buffer_size = 8 << 20;
  column_families.emplace_back(RAFT_STATE_CF, state_options);

  // 快照列族配置
  rocksdb::ColumnFamilyOptions snapshot_options;
  snapshot_options.compression = rocksdb::kSnappyCompression;
  snapshot_options.write_buffer_size = 64 << 20;
  column_families.emplace_back(SNAPSHOT_CF, snapshot_options);

  // 检查列族是否存在
  std::vector<std::string> existing_cfs;
  rocksdb::Status status = rocksdb::DB::ListColumnFamilies(
      rocksdb::Options(), db_path_, &existing_cfs);

  std::vector<rocksdb::ColumnFamilyHandle *> handles;

  if (status.ok() && !existing_cfs.empty()) {
    // 数据库已存在，打开现有列族
    status =
        rocksdb::DB::Open(options_, db_path_, column_families, &handles, &db_);
  } else {
    // 数据库不存在，创建新的列族
    rocksdb::DB* db_ptr = nullptr;
    status = rocksdb::DB::Open(options_, db_path_, &db_ptr);
    if (!status.ok()) {
      LOG_FATAL("Failed to open RocksDB: {}", status.ToString());
      return false;
    }
    db_.reset(db_ptr);

    // 创建列族（跳过默认列族）
    for (size_t i = 1; i < column_families.size(); ++i) {
      const auto &cf_desc = column_families[i];
      rocksdb::ColumnFamilyHandle *handle;
      status = db_->CreateColumnFamily(cf_desc.options, cf_desc.name, &handle);
      if (!status.ok()) {
        LOG_ERROR("Failed to create column family {}: {}", cf_desc.name,
                  status.ToString());
        return false;
      }
      cf_handles_[cf_desc.name] = handle;
    }

    // 重新打开数据库以使用所有列族
    db_.reset();
    handles.clear();
    status =
        rocksdb::DB::Open(options_, db_path_, column_families, &handles, &db_);
  }

  if (!status.ok()) {
    LOG_FATAL("Failed to open RocksDB with column families: {}",
              status.ToString());
    return false;
  }

  // 映射列族句柄
  for (size_t i = 0; i < column_families.size() && i < handles.size(); ++i) {
    const std::string& cf_name = column_families[i].name;
    if (cf_name == rocksdb::kDefaultColumnFamilyName) {
      cf_handles_[DEFAULT_CF] = handles[i];
    } else {
      cf_handles_[cf_name] = handles[i];
    }
  }

  LOG_INFO("RocksDB opened with column families at {}", db_path_);
  return true;
}

bool RocksDBStorage::Open() { return InitColumnFamilies(); }

void RocksDBStorage::Close() {
  if (db_) {
    db_.reset();
    LOG_INFO("RocksDB closed");
  }
}

// KV操作
bool RocksDBStorage::Put(const std::string &key, const std::string &value) {
  if (!db_ || cf_handles_.find(KV_CF) == cf_handles_.end())
    return false;

  rocksdb::Status status =
      db_->Put(write_options_, cf_handles_[KV_CF], key, value);
  LOG_STATUS_IF_ONT_OK("Put", status);
  return status.ok();
}

bool RocksDBStorage::Get(const std::string &key, std::string *value) {
  if (!db_ || !value || cf_handles_.find(KV_CF) == cf_handles_.end())
    return false;

  rocksdb::Status status =
      db_->Get(read_options_, cf_handles_[KV_CF], key, value);
  LOG_STATUS_IF_ONT_OK("Get", status);
  return status.ok();
}

bool RocksDBStorage::Delete(const std::string &key) {
  if (!db_ || cf_handles_.find(KV_CF) == cf_handles_.end())
    return false;

  rocksdb::Status status = db_->Delete(write_options_, cf_handles_[KV_CF], key);
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
  if (!db_ || cf_handles_.find(RAFT_STATE_CF) == cf_handles_.end())
    return false;

  // 序列化Raft状态
  std::stringstream ss;
  ss << term << ":" << voted_for;

  rocksdb::Status status =
      db_->Put(write_options_, cf_handles_[RAFT_STATE_CF], "state", ss.str());
  LOG_STATUS_IF_ONT_OK("SaveRaftState", status);
  return status.ok();
}

bool RocksDBStorage::LoadRaftState(uint64_t *term, std::string *voted_for) {
  if (!db_ || !term || !voted_for ||
      cf_handles_.find(RAFT_STATE_CF) == cf_handles_.end())
    return false;

  std::string value;
  rocksdb::Status status =
      db_->Get(read_options_, cf_handles_[RAFT_STATE_CF], "state", &value);
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
  if (!db_ || cf_handles_.find(RAFT_LOG_CF) == cf_handles_.end()) {
    LOG_ERROR("RocksDB not opened or raft log column family not found");
    return false;
  }

  std::string key = MakeLogKey(entry.index());
  std::string value;
  if (!entry.SerializeToString(&value)) {
    LOG_ERROR("Failed to serialize log entry");
    return false;
  }

  rocksdb::Status status =
      db_->Put(write_options_, cf_handles_[RAFT_LOG_CF], key, value);
  LOG_STATUS_IF_ONT_OK("AppendLog", status);
  return status.ok();
}

bool RocksDBStorage::GetLog(uint64_t index, raft::LogEntry *entry) {
  if (!db_ || !entry || cf_handles_.find(RAFT_LOG_CF) == cf_handles_.end()) {
    LOG_ERROR("RocksDB not opened or entry is null or raft log column family "
              "not found");
    return false;
  }

  std::string key = MakeLogKey(index);
  std::string value;

  rocksdb::Status status =
      db_->Get(read_options_, cf_handles_[RAFT_LOG_CF], key, &value);
  if (!status.ok()) {
    LOG_STATUS_IF_ONT_OK("GetLog", status);
    return false;
  }

  return entry->ParseFromString(value);
}

bool RocksDBStorage::GetLogs(uint64_t start_index, uint64_t end_index,
                             std::vector<raft::LogEntry> *entries) {
  if (!db_ || !entries || start_index > end_index ||
      cf_handles_.find(RAFT_LOG_CF) == cf_handles_.end()) {
    LOG_ERROR("RocksDB not opened or entry is null or Invalid log index range "
              "or raft log column family not found");
    return false;
  }

  entries->clear();

  // 使用迭代器批量读取
  rocksdb::ReadOptions read_opts = read_options_;
  std::unique_ptr<rocksdb::Iterator> iter(
      db_->NewIterator(read_opts, cf_handles_[RAFT_LOG_CF]));

  std::string start_key = MakeLogKey(start_index);
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
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
  if (!db_ || cf_handles_.find(RAFT_LOG_CF) == cf_handles_.end()) {
    LOG_ERROR("RocksDB not opened or raft log column family not found");
    return false;
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_opts = read_options_;
  std::unique_ptr<rocksdb::Iterator> iter(
      db_->NewIterator(read_opts, cf_handles_[RAFT_LOG_CF]));

  std::string start_key = MakeLogKey(index + 1);
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    batch.Delete(cf_handles_[RAFT_LOG_CF], iter->key());
  }

  rocksdb::Status status = db_->Write(write_options_, &batch);
  LOG_STATUS_IF_ONT_OK("DeleteLogsAfter", status);
  return status.ok();
}

bool RocksDBStorage::GetLastLogIndex(uint64_t *index) {
  if (!db_ || !index || cf_handles_.find(RAFT_LOG_CF) == cf_handles_.end()) {
    LOG_ERROR("RocksDB not opened or index is null or raft log column family "
              "not found");
    return false;
  }

  *index = 0;

  // 反向迭代找到最后一个日志条目
  rocksdb::ReadOptions read_opts = read_options_;
  std::unique_ptr<rocksdb::Iterator> iter(
      db_->NewIterator(read_opts, cf_handles_[RAFT_LOG_CF]));

  // 从日志键的最大可能值开始反向查找
  std::string max_log_key = MakeLogKey(999999999999);

  iter->SeekForPrev(max_log_key);

  while (iter->Valid()) {
    uint64_t log_index;
    if (ParseLogKey(iter->key().ToString(), &log_index)) {
      *index = log_index;
      return true;
    }
    iter->Prev();
  }

  return true; // 没有日志也是正常的
}

// 快照相关
bool RocksDBStorage::SaveSnapshot(uint64_t index, uint64_t term,
                                  const std::string &state_machine_data) {
  if (!db_ || cf_handles_.find(SNAPSHOT_CF) == cf_handles_.end() ||
      cf_handles_.find(RAFT_LOG_CF) == cf_handles_.end()) {
    LOG_ERROR("RocksDB not opened or required column families not found");
    return false;
  }

  rocksdb::WriteBatch batch;

  // 保存快照元数据
  std::stringstream meta;
  meta << index << ":" << term;
  batch.Put(cf_handles_[SNAPSHOT_CF], "meta", meta.str());

  // 保存状态机数据
  batch.Put(cf_handles_[SNAPSHOT_CF], "data", state_machine_data);

  // 删除快照之前的日志
  rocksdb::ReadOptions read_opts = read_options_;
  std::unique_ptr<rocksdb::Iterator> iter(
      db_->NewIterator(read_opts, cf_handles_[RAFT_LOG_CF]));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    uint64_t log_index;
    if (ParseLogKey(iter->key().ToString(), &log_index) && log_index <= index) {
      batch.Delete(cf_handles_[RAFT_LOG_CF], iter->key());
    }
  }

  rocksdb::Status status = db_->Write(write_options_, &batch);
  LOG_STATUS_IF_ONT_OK("SaveSnapshot", status);
  return status.ok();
}

bool RocksDBStorage::LoadSnapshot(uint64_t *index, uint64_t *term,
                                  std::string *state_machine_data) {
  if (!db_ || !index || !term || !state_machine_data ||
      cf_handles_.find(SNAPSHOT_CF) == cf_handles_.end()) {
    LOG_ERROR("RocksDB not opened or index, term, or state_machine_data is "
              "null or snapshot column family not found");
    return false;
  }

  // 加载快照元数据
  std::string meta;
  rocksdb::Status status =
      db_->Get(read_options_, cf_handles_[SNAPSHOT_CF], "meta", &meta);
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
  status = db_->Get(read_options_, cf_handles_[SNAPSHOT_CF], "data",
                    state_machine_data);
  LOG_STATUS_IF_ONT_OK("LoadSnapshot", status);
  return status.ok();
}

bool RocksDBStorage::HasSnapshot() {
  if (!db_ || cf_handles_.find(SNAPSHOT_CF) == cf_handles_.end()) {
    LOG_ERROR("RocksDB not opened or snapshot column family not found");
    return false;
  }

  std::string value;
  rocksdb::Status status =
      db_->Get(read_options_, cf_handles_[SNAPSHOT_CF], "meta", &value);
  LOG_STATUS_IF_ONT_OK("HasSnapshot", status);
  return status.ok();
}

bool RocksDBStorage::GetAllKVPairs(std::map<std::string, std::string> *kvs) {
  if (!db_ || !kvs || cf_handles_.find(KV_CF) == cf_handles_.end()) {
    LOG_ERROR(
        "RocksDB not opened or kvs is null or kv column family not found");
    return false;
  }

  kvs->clear();

  rocksdb::ReadOptions read_opts = read_options_;
  std::unique_ptr<rocksdb::Iterator> iter(
      db_->NewIterator(read_opts, cf_handles_[KV_CF]));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    kvs->emplace(iter->key().ToString(), iter->value().ToString());
  }

  return true;
}

// 辅助方法
std::string RocksDBStorage::MakeLogKey(uint64_t index) const {
  std::stringstream ss;
  ss << std::setfill('0') << std::setw(20) << index;
  return ss.str();
}

std::string RocksDBStorage::MakeKVKey(const std::string &key) const {
  return key; // 直接使用原始key，不再需要前缀
}

bool RocksDBStorage::ParseLogKey(const std::string &key,
                                 uint64_t *index) const {
  try {
    *index = std::stoull(key);
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