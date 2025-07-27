// include/rpc/kvstore_service.h
#pragma once

#include "kvstore.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>

namespace raft {
class RaftNode;
}

namespace kvstore {

class KVStoreServiceImpl final : public KVStoreService::Service {
public:
  KVStoreServiceImpl(raft::RaftNode *raft_node);
  ~KVStoreServiceImpl() override = default;

  // Get 操作
  grpc::Status Get(grpc::ServerContext *context, const GetRequest *request,
                   GetReply *reply) override;

  // Put 操作
  grpc::Status Put(grpc::ServerContext *context, const PutRequest *request,
                   PutReply *reply) override;

  // Delete 操作
  grpc::Status Delete(grpc::ServerContext *context,
                      const DeleteRequest *request,
                      DeleteReply *reply) override;

  // TODO:批量操作
  grpc::Status BatchOp(grpc::ServerContext *context,
                       const BatchOpRequest *request,
                       BatchOpReply *reply) override;

private:
  raft::RaftNode *raft_node_;

  // 生成请求ID
  std::string GenerateRequestId();

  // 序列化KV命令
  std::string SerializeCommand(const KVCommand &cmd);
};

// KV存储服务器
class KVStoreServer {
public:
  KVStoreServer(const std::string &listen_address, raft::RaftNode *raft_node);
  ~KVStoreServer() { Stop(); }

  void Start();
  void Stop();
  void Wait();

private:
  std::string listen_address_;
  std::unique_ptr<KVStoreServiceImpl> service_impl_;
  std::unique_ptr<grpc::Server> server_;
};

} // namespace kvstore