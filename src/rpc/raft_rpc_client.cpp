// src/rpc/raft_rpc_client.cpp
#include "rpc/raft_rpc_client.h"
#include "lyf/LogSystem.h"
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

namespace raft {
using std::lock_guard, std::mutex, std::string;

RaftRpcClient::RaftRpcClient() {
  // 设置通道参数
  channel_args_.SetMaxReceiveMessageSize(16 * 1024 * 1024); // 16MB
  channel_args_.SetMaxSendMessageSize(16 * 1024 * 1024);    // 16MB

  // 启用重试
  channel_args_.SetInt(GRPC_ARG_ENABLE_RETRIES, 1);
  channel_args_.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1000);
}

void RaftRpcClient::AddPeer(const string &peer_id, const string &address) {
  lock_guard<mutex> lock(mutex_);

  auto conn = std::make_unique<PeerConnection>();
  conn->address = address;
  conn->channel = CreateChannel(address);
  conn->stub = RaftService::NewStub(conn->channel);

  peers_[peer_id] = std::move(conn);

  LOG_INFO("[RaftRpcClient] Added peer: {} -> {}", peer_id, address);
}

void RaftRpcClient::RemovePeer(const string &peer_id) {
  lock_guard<mutex> lock(mutex_);
  peers_.erase(peer_id);

  LOG_INFO("[RaftRpcClient] Removed peer: {}", peer_id);
}

std::vector<string> RaftRpcClient::GetPeers() const {
  lock_guard<mutex> lock(mutex_);

  std::vector<string> peer_ids;
  peer_ids.reserve(peers_.size());

  for (const auto &[id, _] : peers_) {
    peer_ids.push_back(id);
  }

  return peer_ids;
}

RaftRpcClient::PeerConnection *
RaftRpcClient::GetConnection(const string &peer_id) {
  auto it = peers_.find(peer_id);
  if (it == peers_.end()) {
    return nullptr;
  }
  return it->second.get();
}

std::shared_ptr<grpc::Channel>
RaftRpcClient::CreateChannel(const string &address) {
  return grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(),
                                   channel_args_);
}

void RaftRpcClient::ResetConnection(PeerConnection *conn) {
  LOG_INFO("[RaftRpcClient] Resetting connection to {}", conn->address);

  conn->channel = CreateChannel(conn->address);
  conn->stub = RaftService::NewStub(conn->channel);
}

// 同步 RequestVote
bool RaftRpcClient::SendRequestVote(const string &peer_id,
                                    const RequestVoteRequest &request,
                                    RequestVoteReply *reply,
                                    std::chrono::milliseconds timeout) {
  lock_guard<mutex> lock(mutex_);

  auto *conn = GetConnection(peer_id);
  if (!conn) {
    LOG_ERROR("[RaftRpcClient] Peer not found: {}", peer_id);
    return false;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + timeout);

  conn->request_count++;

  grpc::Status status = conn->stub->RequestVote(&context, request, reply);

  if (!status.ok()) {
    conn->error_count++;
    conn->last_error_time = std::chrono::steady_clock::now();

    // 如果是连接错误，则重置连接
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      ResetConnection(conn);
    }

    LOG_ERROR("[RaftRpcClient] RequestVote to {} failed: {}", peer_id,
              status.error_message());
    return false;
  }

  return true;
}

// 同步 AppendEntries
bool RaftRpcClient::SendAppendEntries(const string &peer_id,
                                      const AppendEntriesRequest &request,
                                      AppendEntriesReply *reply,
                                      std::chrono::milliseconds timeout) {
  lock_guard<mutex> lock(mutex_);

  auto *conn = GetConnection(peer_id);
  if (!conn) {
    return false;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + timeout);

  conn->request_count++;
  grpc::Status status = conn->stub->AppendEntries(&context, request, reply);

  if (!status.ok()) {
    conn->error_count++;
    conn->last_error_time = std::chrono::steady_clock::now();

    // 只在非超时错误时打印
    if (status.error_code() != grpc::StatusCode::DEADLINE_EXCEEDED) {
      LOG_ERROR("[RaftRpcClient] AppendEntries to {} failed: {}", peer_id,
                status.error_message());
    }
    return false;
  }

  return true;
}

// 同步 InstallSnapshot
bool RaftRpcClient::SendInstallSnapshot(const string &peer_id,
                                        const InstallSnapshotRequest &request,
                                        InstallSnapshotReply *reply,
                                        std::chrono::milliseconds timeout) {
  lock_guard<mutex> lock(mutex_);

  auto *conn = GetConnection(peer_id);
  if (!conn) {
    return false;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + timeout);

  conn->request_count++;

  grpc::Status status = conn->stub->InstallSnapshot(&context, request, reply);

  if (!status.ok()) {
    conn->error_count++;
    conn->last_error_time = std::chrono::steady_clock::now();

    LOG_ERROR("[RaftRpcClient] InstallSnapshot to {} failed: {}", peer_id,
              status.error_message());
    return false;
  }

  return true;
}

// 异步 RequestVote
std::future<std::pair<bool, RequestVoteReply>>
RaftRpcClient::AsyncRequestVote(const string &peer_id,
                                const RequestVoteRequest &request) {
  return std::async(std::launch::async, [this, peer_id, request]() {
    RequestVoteReply reply;
    bool success = SendRequestVote(peer_id, request, &reply);
    return std::make_pair(success, reply);
  });
}

// 异步 AppendEntries
std::future<std::pair<bool, AppendEntriesReply>>
RaftRpcClient::AsyncAppendEntries(const string &peer_id,
                                  const AppendEntriesRequest &request) {
  return std::async(std::launch::async, [this, peer_id, request]() {
    AppendEntriesReply reply;
    bool success = SendAppendEntries(peer_id, request, &reply);
    return std::make_pair(success, reply);
  });
}

// 异步 InstallSnapshot
std::future<std::pair<bool, InstallSnapshotReply>>
RaftRpcClient::AsyncInstallSnapshot(const string &peer_id,
                                    const InstallSnapshotRequest &request) {
  return std::async(std::launch::async, [this, peer_id, request]() {
    InstallSnapshotReply reply;
    bool success = SendInstallSnapshot(peer_id, request, &reply);
    return std::make_pair(success, reply);
  });
}

} // namespace raft