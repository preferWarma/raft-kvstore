// tests/test_rpc_communication.cpp
#include "raft/raft_state.h"
#include "rpc/raft_rpc_client.h"
#include "rpc/raft_rpc_server.h"
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <thread>

using namespace raft;

class RpcCommunicationTest : public ::testing::Test {
protected:
  void SetUp() override {
    // 创建测试目录
    test_dir_ = "./test_rpc_" + std::to_string(getpid());
    std::filesystem::remove_all(test_dir_);
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    // 清理测试目录
    std::filesystem::remove_all(test_dir_);
  }

  std::string test_dir_;
};

// 测试基本的RPC服务器启动和停止
TEST_F(RpcCommunicationTest, ServerStartStop) {
  std::vector<std::string> peers;
  RaftNode node("node1", peers, test_dir_);

  RaftRpcServer server("127.0.0.1:50051", &node);

  EXPECT_FALSE(server.IsRunning());

  server.Start();
  EXPECT_TRUE(server.IsRunning());

  // 等待一下确保服务器完全启动
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  server.Stop();
  EXPECT_FALSE(server.IsRunning());
}

// 测试客户端连接管理
TEST_F(RpcCommunicationTest, ClientPeerManagement) {
  RaftRpcClient client;

  EXPECT_EQ(client.GetPeers().size(), 0);

  client.AddPeer("node2", "127.0.0.1:50052");
  client.AddPeer("node3", "127.0.0.1:50053");

  auto peers = client.GetPeers();
  EXPECT_EQ(peers.size(), 2);

  client.RemovePeer("node2");
  peers = client.GetPeers();
  EXPECT_EQ(peers.size(), 1);
}

// 测试RequestVote RPC
TEST_F(RpcCommunicationTest, RequestVoteRpc) {
  // 创建两个节点
  std::vector<std::string> peers1 = {"node2"};
  std::vector<std::string> peers2 = {"node1"};

  RaftNode node1("node1", peers1, test_dir_ + "/node1");
  RaftNode node2("node2", peers2, test_dir_ + "/node2");

  // 启动RPC服务器
  RaftRpcServer server1("127.0.0.1:50051", &node1);
  RaftRpcServer server2("127.0.0.1:50052", &node2);

  server1.Start();
  server2.Start();

  // 等待服务器启动
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // 创建客户端
  RaftRpcClient client;
  client.AddPeer("node2", "127.0.0.1:50052");

  // 发送RequestVote
  RequestVoteRequest request;
  request.set_term(1);
  request.set_candidate_id("node1");
  request.set_last_log_index(0);
  request.set_last_log_term(0);

  RequestVoteReply reply;
  bool success = client.SendRequestVote("node2", request, &reply);

  EXPECT_TRUE(success);
  EXPECT_TRUE(reply.vote_granted()); // 应该投票给第一个请求者

  // 清理
  server1.Stop();
  server2.Stop();
}

// 测试AppendEntries RPC
TEST_F(RpcCommunicationTest, AppendEntriesRpc) {
  // 创建两个节点
  std::vector<std::string> peers1 = {"node2"};
  std::vector<std::string> peers2 = {"node1"};

  RaftNode node1("node1", peers1, test_dir_ + "/node1");
  RaftNode node2("node2", peers2, test_dir_ + "/node2");

  // 启动RPC服务器
  RaftRpcServer server1("127.0.0.1:50051", &node1);
  RaftRpcServer server2("127.0.0.1:50052", &node2);

  server1.Start();
  server2.Start();

  // 等待服务器启动
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // 创建客户端
  RaftRpcClient client;
  client.AddPeer("node2", "127.0.0.1:50052");

  // 发送AppendEntries（心跳）
  AppendEntriesRequest request;
  request.set_term(1);
  request.set_leader_id("node1");
  request.set_prev_log_index(0);
  request.set_prev_log_term(0);
  request.set_leader_commit(0);

  AppendEntriesReply reply;
  bool success = client.SendAppendEntries("node2", request, &reply);

  EXPECT_TRUE(success);
  EXPECT_TRUE(reply.success());

  // 清理
  server1.Stop();
  server2.Stop();
}

// 测试异步RPC
TEST_F(RpcCommunicationTest, AsyncRpc) {
  // 创建节点和服务器
  std::vector<std::string> peers = {"node2", "node3"};
  RaftNode node("node1", peers, test_dir_);

  RaftRpcServer server2("127.0.0.1:50052", &node);
  RaftRpcServer server3("127.0.0.1:50053", &node);

  server2.Start();
  server3.Start();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // 创建客户端
  RaftRpcClient client;
  client.AddPeer("node2", "127.0.0.1:50052");
  client.AddPeer("node3", "127.0.0.1:50053");

  // 异步发送RequestVote
  RequestVoteRequest request;
  request.set_term(1);
  request.set_candidate_id("node1");
  request.set_last_log_index(0);
  request.set_last_log_term(0);

  auto future2 = client.AsyncRequestVote("node2", request);
  auto future3 = client.AsyncRequestVote("node3", request);

  // 等待结果
  auto result2 = future2.get();
  auto result3 = future3.get();

  EXPECT_TRUE(result2.first); // RPC成功
  EXPECT_TRUE(result3.first); // RPC成功

  // 清理
  server2.Stop();
  server3.Stop();
}

// 测试RPC超时
TEST_F(RpcCommunicationTest, RpcTimeout) {
  RaftRpcClient client;
  client.AddPeer("node2", "127.0.0.1:50052"); // 没有服务器监听这个地址

  RequestVoteRequest request;
  request.set_term(1);
  request.set_candidate_id("node1");

  RequestVoteReply reply;
  auto start = std::chrono::steady_clock::now();
  bool success = client.SendRequestVote("node2", request, &reply,
                                        std::chrono::milliseconds(100));
  auto end = std::chrono::steady_clock::now();

  EXPECT_FALSE(success); // 应该失败

  // 验证超时时间
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  EXPECT_LE(duration.count(), 200); // 应该在200ms内超时
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}