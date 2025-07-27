// include/rpc/kv_client.h
#pragma once

#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

namespace kvstore {
using std::string;

class KVClient {
public:
  explicit KVClient(const std::string &server_address);
  ~KVClient() = default;

  // KV操作
  bool Put(const string &key, const string &value);
  bool Get(const string &key, string *value);
  bool Delete(const string &key);

  // 获取最后的错误
  string GetLastError() const { return last_error_; }

  // 获取当前leader提示
  string GetLeaderHint() const { return leader_hint_; }

private:
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<KVStoreService::Stub> stub_;

  string last_error_;
  string leader_hint_;
};

} // namespace kvstore