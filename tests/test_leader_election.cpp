// tests/test_leader_election.cpp
#include "raft/raft_state.h"
#include "rpc/raft_rpc_client.h"
#include "rpc/raft_rpc_server.h"
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

class LeaderElectionTest : public ::testing::Test {
protected:
  struct TestNode {
    std::string id;
    std::string addr;
    std::unique_ptr<raft::RaftNode> node;
    std::unique_ptr<raft::RaftRpcServer> server;
    bool is_running = true;

    TestNode(const std::string &node_id, const std::string &address,
             const std::vector<std::string> &peers,
             const std::map<std::string, std::string> &peer_addrs)
        : id(node_id), addr(address) {

      // 创建节点
      node = std::make_unique<raft::RaftNode>(id, peers, "./test_data/" + id);

      // 设置RPC客户端
      auto client = std::make_unique<raft::RaftRpcClient>();
      for (const auto &[peer_id, peer_addr] : peer_addrs) {
        if (peer_id != id) {
          client->AddPeer(peer_id, peer_addr);
        }
      }
      node->SetRpcClient(std::move(client));

      // 创建RPC服务器
      server = std::make_unique<raft::RaftRpcServer>(addr, node.get());
    }

    void Start() {
      is_running = true;
      server->Start();
      node->Start();
    }

    void Stop() {
      is_running = false;
      node->Stop();
      server->Stop();
    }
  };

  void SetUp() override {
    // 清理测试目录
    system("rm -rf ./test_data");
    system("mkdir -p ./test_data");
  }

  void TearDown() override {
    // 停止所有节点
    for (auto &node : nodes_) {
      node->Stop();
    }
    nodes_.clear();
  }

  void CreateCluster(int size) {
    std::map<std::string, std::string> addrs;
    std::vector<std::string> ids;

    // 生成地址
    for (int i = 1; i <= size; i++) {
      std::string id = "node" + std::to_string(i);
      std::string addr = "127.0.0.1:" + std::to_string(9000 + i);
      addrs[id] = addr;
      ids.push_back(id);
    }

    // 创建节点
    for (const auto &[id, addr] : addrs) {
      std::vector<std::string> peers;
      for (const auto &peer_id : ids) {
        if (peer_id != id) {
          peers.push_back(peer_id);
        }
      }

      nodes_.push_back(std::make_unique<TestNode>(id, addr, peers, addrs));
    }

    // 启动所有节点
    for (auto &node : nodes_) {
      node->Start();
    }

    // 等待节点初始化
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  raft::RaftNode *FindLeader() {
    for (auto &node : nodes_) {
      if (node->is_running && node->node->IsLeader()) {
        return node->node.get();
      }
    }
    return nullptr;
  }

  int CountLeaders() {
    int count = 0;
    for (auto &node : nodes_) {
      if (node->is_running && node->node->IsLeader()) {
        count++;
      }
    }
    return count;
  }

  std::vector<std::unique_ptr<TestNode>> nodes_;
};

TEST_F(LeaderElectionTest, SingleLeaderElected) {
  CreateCluster(3);

  // 等待选举完成（最多5秒）
  auto start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - start < std::chrono::seconds(5)) {
    if (FindLeader() != nullptr) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // 验证只有一个leader
  EXPECT_EQ(CountLeaders(), 1);

  auto *leader = FindLeader();
  ASSERT_NE(leader, nullptr);

  // 验证所有follower都知道leader
  for (auto &node : nodes_) {
    if (!node->node->IsLeader()) {
      EXPECT_EQ(node->node->GetLeaderId(), leader->GetNodeId());
    }
  }
}

TEST_F(LeaderElectionTest, LeaderReelection) {
  CreateCluster(5);

  // 等待初始选举
  std::this_thread::sleep_for(std::chrono::seconds(2));

  auto *initial_leader = FindLeader();
  ASSERT_NE(initial_leader, nullptr);
  auto initial_leader_id = initial_leader->GetNodeId();

  std::cout << "Initial leader: " << initial_leader_id << std::endl;

  // 停止当前leader
  for (auto &node : nodes_) {
    if (node->node.get() == initial_leader) {
      node->Stop();
      break;
    }
  }

  std::cout << "Stopped leader, waiting for re-election..." << std::endl;

  // 等待新的选举（最多10秒）
  auto start = std::chrono::steady_clock::now();
  raft::RaftNode *new_leader = nullptr;

  while (std::chrono::steady_clock::now() - start < std::chrono::seconds(10)) {
    new_leader = FindLeader();
    if (new_leader != nullptr && new_leader != initial_leader) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // 验证新leader被选出
  ASSERT_NE(new_leader, nullptr);
  EXPECT_NE(new_leader->GetNodeId(), initial_leader_id);

  std::cout << "New leader: " << new_leader->GetNodeId() << std::endl;

  // 验证仍然只有一个leader
  EXPECT_EQ(CountLeaders(), 1);
}

TEST_F(LeaderElectionTest, SplitVoteResolution) {
  CreateCluster(4); // 偶数节点更容易产生split vote

  // 等待选举完成
  std::this_thread::sleep_for(std::chrono::seconds(5));

  // 即使有split vote，最终也应该选出leader
  EXPECT_EQ(CountLeaders(), 1);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}