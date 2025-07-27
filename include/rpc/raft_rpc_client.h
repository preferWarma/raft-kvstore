// include/rpc/raft_rpc_client.h
#pragma once

#include "raft.grpc.pb.h"
#include <chrono>
#include <future>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace raft {
using std::string, std::vector;
namespace chrono = std::chrono;

// RPC客户端，管理与其他Raft节点的连接
class RaftRpcClient {
public:
  RaftRpcClient();
  ~RaftRpcClient() = default;

  // 添加对等节点
  void AddPeer(const string &peer_id, const string &address);

  // 移除对等节点
  void RemovePeer(const string &peer_id);

  // 获取所有对等节点
  vector<string> GetPeers() const;

  // 同步RPC调用（带超时）
  bool SendRequestVote(const string &peer_id, const RequestVoteRequest &request,
                       RequestVoteReply *reply,
                       chrono::milliseconds timeout = chrono::milliseconds(50));

  bool
  SendAppendEntries(const string &peer_id, const AppendEntriesRequest &request,
                    AppendEntriesReply *reply,
                    chrono::milliseconds timeout = chrono::milliseconds(50));

  bool SendInstallSnapshot(
      const string &peer_id, const InstallSnapshotRequest &request,
      InstallSnapshotReply *reply,
      chrono::milliseconds timeout = chrono::milliseconds(5000));

  // 异步RPC调用
  std::future<std::pair<bool, RequestVoteReply>>
  AsyncRequestVote(const string &peer_id, const RequestVoteRequest &request);

  std::future<std::pair<bool, AppendEntriesReply>>
  AsyncAppendEntries(const string &peer_id,
                     const AppendEntriesRequest &request);

  std::future<std::pair<bool, InstallSnapshotReply>>
  AsyncInstallSnapshot(const string &peer_id,
                       const InstallSnapshotRequest &request);

private:
  // 对等节点连接信息
  struct PeerConnection {
    string address;
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<RaftService::Stub> stub;

    // 连接统计
    std::atomic<uint64_t> request_count{0};
    std::atomic<uint64_t> error_count{0};
    chrono::steady_clock::time_point last_error_time;
  };

  // 获取或创建连接
  PeerConnection *GetConnection(const string &peer_id);

  // 重置连接
  void ResetConnection(PeerConnection *conn);

  // 创建gRPC通道
  std::shared_ptr<grpc::Channel> CreateChannel(const string &address);

private:
  mutable std::mutex mutex_;
  std::unordered_map<string, std::unique_ptr<PeerConnection>> peers_;

  // 通道参数
  grpc::ChannelArguments channel_args_;
};

} // namespace raft