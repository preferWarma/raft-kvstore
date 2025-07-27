// src/rpc/kv_client.cpp
#include "rpc/kv_client.h"

namespace kvstore {
using std::string;
namespace chrono = std::chrono;

KVClient::KVClient(const string &server_address) {
  channel_ =
      grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
  stub_ = KVStoreService::NewStub(channel_);
}

bool KVClient::Put(const string &key, const string &value) {
  PutRequest request;
  request.set_key(key);
  request.set_value(value);

  PutReply reply;
  grpc::ClientContext context;
  context.set_deadline(chrono::system_clock::now() + chrono::seconds(5));

  grpc::Status status = stub_->Put(&context, request, &reply);

  if (!status.ok()) {
    last_error_ = status.error_message();
    return false;
  }

  if (!reply.success()) {
    last_error_ = reply.error();
    leader_hint_ = reply.leader_hint();
    return false;
  }

  return true;
}

bool KVClient::Get(const string &key, string *value) {
  GetRequest request;
  request.set_key(key);
  request.set_linearizable(true);

  GetReply reply;
  grpc::ClientContext context;
  context.set_deadline(chrono::system_clock::now() + chrono::seconds(5));

  grpc::Status status = stub_->Get(&context, request, &reply);

  if (!status.ok()) {
    last_error_ = status.error_message();
    return false;
  }

  if (!reply.success()) {
    last_error_ = reply.error();
    leader_hint_ = reply.leader_hint();
    return false;
  }

  *value = reply.value();
  return true;
}

bool KVClient::Delete(const string &key) {
  DeleteRequest request;
  request.set_key(key);

  DeleteReply reply;
  grpc::ClientContext context;
  context.set_deadline(chrono::system_clock::now() + chrono::seconds(5));

  grpc::Status status = stub_->Delete(&context, request, &reply);

  if (!status.ok()) {
    last_error_ = status.error_message();
    return false;
  }

  if (!reply.success()) {
    last_error_ = reply.error();
    leader_hint_ = reply.leader_hint();
    return false;
  }

  return true;
}

} // namespace kvstore