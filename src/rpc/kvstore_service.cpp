// src/rpc/kvstore_service.cpp
#include "rpc/kvstore_service.h"
#include "lyf/LogSystem.h"
#include "raft/raft_state.h"
#include <iomanip>
#include <random>
#include <sstream>

namespace kvstore {

KVStoreServiceImpl::KVStoreServiceImpl(raft::RaftNode *raft_node)
    : raft_node_(raft_node) {
  if (!raft_node_) {
    LOG_FATAL("raft_node_ is null");
    throw std::invalid_argument("RaftNode cannot be null");
  }
}

std::string KVStoreServiceImpl::GenerateRequestId() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 999999);

  std::stringstream ss;
  ss << std::setfill('0') << std::setw(6) << dis(gen);
  return ss.str();
}

std::string KVStoreServiceImpl::SerializeCommand(const KVCommand &cmd) {
  std::string serialized;
  cmd.SerializeToString(&serialized);
  return serialized;
}

grpc::Status KVStoreServiceImpl::Get(grpc::ServerContext *context,
                                     const GetRequest *request,
                                     GetReply *reply) {
  if (request->linearizable()) {
    if (!raft_node_->IsLeader()) {
      LOG_WARN("node_id: {} is not leader, node_id: {} is leader",
               raft_node_->GetNodeId(), raft_node_->GetLeaderId());
      reply->set_success(false);
      reply->set_error("Not leader");
      reply->set_leader_hint(raft_node_->GetLeaderId());
      return grpc::Status::OK;
    }
  }

  // 使用布隆过滤器检查键是否存在
  if (!raft_node_->GetStateMachine()->MayExistInBloomFilter(request->key())) {
    LOG_DEBUG("key: {} not found in bloom filter", request->key().c_str());
    reply->set_success(false);
    reply->set_error("Key not found");
    return grpc::Status::OK;
  }

  // 从状态机读取数据
  std::string value;
  if (raft_node_->GetStateMachine()->Get(request->key(), &value)) {
    reply->set_success(true);
    reply->set_value(value);
  } else {
    LOG_WARN("key: {} not found", request->key().c_str());
    reply->set_success(false);
    reply->set_error("Key not found");
  }

  return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::Put(grpc::ServerContext *context,
                                     const PutRequest *request,
                                     PutReply *reply) {
  if (!raft_node_->IsLeader()) {
    LOG_WARN("node_id: {} is not leader, node_id: {} is leader",
             raft_node_->GetNodeId(), raft_node_->GetLeaderId());
    reply->set_success(false);
    reply->set_error("Not leader");
    reply->set_leader_hint(raft_node_->GetLeaderId());
    return grpc::Status::OK;
  }

  // 构造命令
  KVCommand cmd;
  cmd.set_type(KVCommand::PUT);
  cmd.set_key(request->key());
  cmd.set_value(request->value());
  cmd.set_request_id(GenerateRequestId());

  // 提交到Raft
  auto future =
      raft_node_->SubmitCommand(SerializeCommand(cmd), cmd.request_id());

  // 等待结果（带超时）
  if (future.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
    bool success = future.get();
    reply->set_success(success);
    if (!success) {
      LOG_ERROR("Failed to commit, request_id: {}", cmd.request_id());
      reply->set_error("Failed to commit");
    }
  } else {
    LOG_ERROR("Timeout, request_id: {}", cmd.request_id());
    reply->set_success(false);
    reply->set_error("Timeout");
  }

  return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::Delete(grpc::ServerContext *context,
                                        const DeleteRequest *request,
                                        DeleteReply *reply) {
  if (!raft_node_->IsLeader()) {
    LOG_WARN("node_id: {} is not leader, node_id: {} is leader",
             raft_node_->GetNodeId(), raft_node_->GetLeaderId());
    reply->set_success(false);
    reply->set_error("Not leader");
    reply->set_leader_hint(raft_node_->GetLeaderId());
    return grpc::Status::OK;
  }

  // 构造命令
  KVCommand cmd;
  cmd.set_type(KVCommand::DELETE);
  cmd.set_key(request->key());
  cmd.set_request_id(GenerateRequestId());

  // 提交到Raft
  auto future =
      raft_node_->SubmitCommand(SerializeCommand(cmd), cmd.request_id());

  // 等待结果
  if (future.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
    bool success = future.get();
    reply->set_success(success);
    if (!success) {
      LOG_ERROR("Failed to commit, request_id: {}", cmd.request_id());
      reply->set_error("Failed to commit");
    }
  } else {
    LOG_ERROR("Timeout, request_id: {}", cmd.request_id());
    reply->set_success(false);
    reply->set_error("Timeout");
  }

  return grpc::Status::OK;
}

grpc::Status KVStoreServiceImpl::BatchOp(grpc::ServerContext *context,
                                         const BatchOpRequest *request,
                                         BatchOpReply *reply) {
  // TODO: 实现批量操作
  LOG_ERROR("BatchOp not implemented");
  reply->set_success(false);
  reply->set_error("Not implemented");
  return grpc::Status::OK;
}

// KVStoreServer 实现
KVStoreServer::KVStoreServer(const std::string &listen_address,
                             raft::RaftNode *raft_node)
    : listen_address_(listen_address) {
  service_impl_ = std::make_unique<KVStoreServiceImpl>(raft_node);
}

void KVStoreServer::Start() {
  grpc::ServerBuilder builder;
  builder.AddListeningPort(listen_address_, grpc::InsecureServerCredentials());
  builder.RegisterService(service_impl_.get());

  server_ = builder.BuildAndStart();
  if (!server_) {
    LOG_FATAL("Failed to start KV store server on {}", listen_address_);
  }

  LOG_INFO("[KVStoreServer] Server listening on {}", listen_address_);
}

void KVStoreServer::Stop() {
  if (server_) {
    server_->Shutdown();
    server_.reset();
  }
}

void KVStoreServer::Wait() {
  if (server_) {
    server_->Wait();
  }
}

} // namespace kvstore