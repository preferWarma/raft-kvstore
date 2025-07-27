// src/rpc/raft_rpc_server.cpp
#include "rpc/raft_rpc_server.h"
#include "lyf/LogSystem.h"
#include "raft/raft_state.h"
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>

namespace raft {

// RaftRpcServiceImpl 实现
RaftRpcServiceImpl::RaftRpcServiceImpl(RaftNode *node) : raft_node_(node) {
  if (!raft_node_) {
    LOG_FATAL("[RaftRpcServiceImpl] RaftNode cannot be null");
    throw std::invalid_argument("RaftNode cannot be null");
  }
}

grpc::Status RaftRpcServiceImpl::RequestVote(grpc::ServerContext *context,
                                             const RequestVoteRequest *request,
                                             RequestVoteReply *reply) {
  // 记录请求
  ++request_vote_count_;

  try {
    // 调用RaftNode处理请求
    raft_node_->HandleRequestVote(request, reply);

    LOG_DEBUG("[RPC] RequestVote: term={}, candidate={}, granted={}",
              request->term(), request->candidate_id(),
              reply->vote_granted() ? "true" : "false");

    return grpc::Status::OK;

  } catch (const std::exception &e) {
    LOG_ERROR("[RPC] RequestVote error: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status
RaftRpcServiceImpl::AppendEntries(grpc::ServerContext *context,
                                  const AppendEntriesRequest *request,
                                  AppendEntriesReply *reply) {
  // 记录请求
  ++append_entries_count_;

  try {
    // 调用RaftNode处理请求
    raft_node_->HandleAppendEntries(request, reply);

    // 只在非心跳时打印日志
    if (request->entries_size() > 0) {
      LOG_DEBUG(
          "[RPC] AppendEntries: term={}, leader={}, entries={}, success={}",
          request->term(), request->leader_id(), request->entries_size(),
          reply->success() ? "true" : "false");
    }

    return grpc::Status::OK;

  } catch (const std::exception &e) {
    LOG_ERROR("[RPC] AppendEntries error: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status
RaftRpcServiceImpl::InstallSnapshot(grpc::ServerContext *context,
                                    const InstallSnapshotRequest *request,
                                    InstallSnapshotReply *reply) {
  // 记录请求
  ++install_snapshot_count_;

  try {
    // 调用RaftNode处理请求
    raft_node_->HandleInstallSnapshot(request, reply);

    LOG_DEBUG("[RPC] InstallSnapshot: term={}, leader={}, lastIndex={}, "
              "done={}",
              request->term(), request->leader_id(),
              request->last_included_index(),
              request->done() ? "true" : "false");

    return grpc::Status::OK;

  } catch (const std::exception &e) {
    LOG_ERROR("[RPC] InstallSnapshot error: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

// RaftRpcServer 实现
RaftRpcServer::RaftRpcServer(const std::string &listen_address, RaftNode *node)
    : listen_address_(listen_address), raft_node_(node) {
  if (!raft_node_) {
    LOG_FATAL("[RaftRpcServer] RaftNode cannot be null");
    throw std::invalid_argument("RaftNode cannot be null");
  }

  service_impl_ = std::make_unique<RaftRpcServiceImpl>(raft_node_);
}

void RaftRpcServer::Start() {
  if (running_) {
    return;
  }

  grpc::ServerBuilder builder;

  // 配置服务器
  builder.AddListeningPort(listen_address_, grpc::InsecureServerCredentials());
  builder.RegisterService(service_impl_.get());

  // 设置选项
  builder.SetMaxReceiveMessageSize(16 * 1024 * 1024); // 16MB
  builder.SetMaxSendMessageSize(16 * 1024 * 1024);    // 16MB

  // 构建并启动服务器
  server_ = builder.BuildAndStart();

  if (!server_) {
    LOG_FATAL("[RaftRpcServer] Failed to start RPC server on {}",
              listen_address_);
  }

  running_ = true;
  LOG_INFO("[RaftRpcServer] Server listening on {}", listen_address_);
}

void RaftRpcServer::Stop() {
  if (!running_) {
    return;
  }

  running_ = false;

  if (server_) {
    // 优雅关闭
    server_->Shutdown();
    server_.reset();
  }

  LOG_INFO("[RaftRpcServer] Server stopped");
}

void RaftRpcServer::Wait() {
  if (server_) {
    server_->Wait();
  }
}

} // namespace raft