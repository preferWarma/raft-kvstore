// include/rpc/raft_rpc_server.h
#pragma once

#include "raft.grpc.pb.h"
#include <atomic>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

namespace raft {

class RaftNode; // 前向声明

// Raft RPC服务实现
class RaftRpcServiceImpl final : public RaftService::Service {
public:
  explicit RaftRpcServiceImpl(RaftNode *node);
  ~RaftRpcServiceImpl() override = default;

  // RequestVote RPC
  grpc::Status RequestVote(grpc::ServerContext *context,
                           const RequestVoteRequest *request,
                           RequestVoteReply *reply) override;

  // AppendEntries RPC
  grpc::Status AppendEntries(grpc::ServerContext *context,
                             const AppendEntriesRequest *request,
                             AppendEntriesReply *reply) override;

  // InstallSnapshot RPC
  grpc::Status InstallSnapshot(grpc::ServerContext *context,
                               const InstallSnapshotRequest *request,
                               InstallSnapshotReply *reply) override;

private:
  RaftNode *raft_node_; // 不拥有所有权

  // 统计信息
  std::atomic<uint64_t> request_vote_count_{0};
  std::atomic<uint64_t> append_entries_count_{0};
  std::atomic<uint64_t> install_snapshot_count_{0};
};

// RPC服务器
class RaftRpcServer {
public:
  RaftRpcServer(const std::string &listen_address, RaftNode *node);
  ~RaftRpcServer() { Stop(); }

  // 启动服务器
  void Start();

  // 停止服务器
  void Stop();

  // 等待服务器关闭
  void Wait();

  // 获取监听地址
  std::string GetListenAddress() const { return listen_address_; }

  // 是否正在运行
  bool IsRunning() const { return running_; }

private:
  std::string listen_address_;
  RaftNode *raft_node_;

  std::unique_ptr<RaftRpcServiceImpl> service_impl_;
  std::unique_ptr<grpc::Server> server_;

  std::atomic<bool> running_{false};
};

} // namespace raft