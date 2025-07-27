// tests/test_kv_integration.cpp
#include "raft/raft_state.h"
#include "rpc/kv_client.h"
#include "rpc/kvstore_service.h"
#include "rpc/raft_rpc_server.h"
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

class KVIntegrationTest : public ::testing::Test {
protected:
  struct TestNode {
    std::string id;
    std::unique_ptr<raft::RaftNode> raft_node;
    std::unique_ptr<raft::RaftRpcServer> raft_server;
    std::unique_ptr<kvstore::KVStoreServer> kv_server;

    TestNode(const std::string &node_id, const std::string &raft_addr,
             const std::string &kv_addr, const std::vector<std::string> &peers,
             const std::map<std::string, std::string> &peer_addrs)
        : id(node_id) {

      // 创建Raft节点
      raft_node =
          std::make_unique<raft::RaftNode>(id, peers, "./test_kv/" + id);

      // 设置RPC客户端
      auto client = std::make_unique<raft::RaftRpcClient>();
      for (const auto &[peer_id, peer_addr] : peer_addrs) {
        if (peer_id != id) {
          client->AddPeer(peer_id, peer_addr);
        }
      }
      raft_node->SetRpcClient(std::move(client));

      // 创建服务器
      raft_server =
          std::make_unique<raft::RaftRpcServer>(raft_addr, raft_node.get());
      kv_server =
          std::make_unique<kvstore::KVStoreServer>(kv_addr, raft_node.get());
    }

    void Start() {
      raft_server->Start();
      kv_server->Start();
      raft_node->Start();
    }

    void Stop() {
      raft_node->Stop();
      raft_server->Stop();
      kv_server->Stop();
    }
  };

  void SetUp() override {
    system("rm -rf ./test_kv");
    system("mkdir -p ./test_kv");
  }

  void TearDown() override {
    for (auto &node : nodes_) {
      node->Stop();
    }
    nodes_.clear();
  }

  void CreateCluster(int size) {
    for (int i = 1; i <= size; i++) {
      std::string id = "node" + std::to_string(i);
      std::string raft_addr = "127.0.0.1:" + std::to_string(20000 + i);
      std::string kv_addr = "127.0.0.1:" + std::to_string(21000 + i);

      std::vector<std::string> peers;
      std::map<std::string, std::string> peer_addrs;

      for (int j = 1; j <= size; j++) {
        if (j != i) {
          std::string peer_id = "node" + std::to_string(j);
          std::string peer_addr = "127.0.0.1:" + std::to_string(20000 + j);
          peers.push_back(peer_id);
          peer_addrs[peer_id] = peer_addr;
        }
      }

      nodes_.push_back(std::make_unique<TestNode>(id, raft_addr, kv_addr, peers,
                                                  peer_addrs));
    }

    for (auto &node : nodes_) {
      node->Start();
    }

    // 等待选举完成
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  std::string FindLeaderKVAddr() {
    for (size_t i = 0; i < nodes_.size(); i++) {
      if (nodes_[i]->raft_node->IsRunning() &&
          nodes_[i]->raft_node->IsLeader()) {
        return "127.0.0.1:" + std::to_string(21001 + i);
      }
    }
    return "";
  }

  std::vector<std::unique_ptr<TestNode>> nodes_;
};

TEST_F(KVIntegrationTest, BasicKVOperations) {
  CreateCluster(3);

  std::string leader_addr = FindLeaderKVAddr();
  ASSERT_FALSE(leader_addr.empty());

  kvstore::KVClient client(leader_addr);

  // Put操作
  EXPECT_TRUE(client.Put("key1", "value1"));
  EXPECT_TRUE(client.Put("key2", "value2"));
  EXPECT_TRUE(client.Put("key3", "value3"));

  // Get操作
  std::string value;
  EXPECT_TRUE(client.Get("key1", &value));
  EXPECT_EQ(value, "value1");

  EXPECT_TRUE(client.Get("key2", &value));
  EXPECT_EQ(value, "value2");

  // 更新
  EXPECT_TRUE(client.Put("key1", "new_value1"));
  EXPECT_TRUE(client.Get("key1", &value));
  EXPECT_EQ(value, "new_value1");

  // Delete操作
  EXPECT_TRUE(client.Delete("key2"));
  EXPECT_FALSE(client.Get("key2", &value));

  // 不存在的键
  EXPECT_FALSE(client.Get("nonexistent", &value));
}

TEST_F(KVIntegrationTest, Persistence) {
  CreateCluster(3);

  std::string leader_addr = FindLeaderKVAddr();
  kvstore::KVClient client(leader_addr);

  // 写入数据
  for (int i = 0; i < 10; i++) {
    std::string key = "persist_key_" + std::to_string(i);
    std::string value = "persist_value_" + std::to_string(i);
    EXPECT_TRUE(client.Put(key, value));
  }

  // 停止所有节点
  for (auto &node : nodes_) {
    node->Stop();
  }

  // 重启所有节点
  for (auto &node : nodes_) {
    node->Start();
  }

  // 等待选举
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // 验证数据还在
  leader_addr = FindLeaderKVAddr();
  kvstore::KVClient new_client(leader_addr);

  for (int i = 0; i < 10; i++) {
    std::string key = "persist_key_" + std::to_string(i);
    std::string expected_value = "persist_value_" + std::to_string(i);
    std::string value;

    EXPECT_TRUE(new_client.Get(key, &value));
    EXPECT_EQ(value, expected_value);
  }
}

TEST_F(KVIntegrationTest, LeaderFailure) {
  CreateCluster(3);

  // 1. 找到初始leader并写入数据
  std::string leader_addr = FindLeaderKVAddr();
  ASSERT_FALSE(leader_addr.empty());
  kvstore::KVClient client(leader_addr);
  EXPECT_TRUE(client.Put("key_before_failure", "value_before_failure"));

  // 2. 找到并停止leader节点
  int leader_idx = -1;
  for (int i = 0; i < nodes_.size(); ++i) {
    if (nodes_[i]->raft_node->IsLeader()) {
      leader_idx = i;
      break;
    }
  }
  ASSERT_NE(leader_idx, -1);
  printf("Stopping leader: %s\n", nodes_[leader_idx]->id.c_str());
  nodes_[leader_idx]->Stop();

  // 3. 等待新leader选举
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // 4. 找到新leader并验证数据
  std::string new_leader_addr = FindLeaderKVAddr();
  ASSERT_FALSE(new_leader_addr.empty());
  ASSERT_NE(new_leader_addr, leader_addr);

  kvstore::KVClient new_client(new_leader_addr);

  // 验证旧数据
  std::string value;
  EXPECT_TRUE(new_client.Get("key_before_failure", &value));
  EXPECT_EQ(value, "value_before_failure");

  // 写入新数据
  EXPECT_TRUE(new_client.Put("key_after_failure", "value_after_failure"));
  EXPECT_TRUE(new_client.Get("key_after_failure", &value));
  EXPECT_EQ(value, "value_after_failure");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}