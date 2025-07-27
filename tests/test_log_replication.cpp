// tests/test_log_replication.cpp
#include "raft/log_manager.h"
#include "raft/raft_state.h"
#include "rpc/raft_rpc_client.h"
#include "rpc/raft_rpc_server.h"
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

class LogReplicationTest : public ::testing::Test {
protected:
  struct TestNode {
    std::string id;
    std::unique_ptr<raft::RaftNode> node;
    std::unique_ptr<raft::RaftRpcServer> server;
    bool is_running = true;

    TestNode(const std::string &node_id, const std::string &addr,
             const std::vector<std::string> &peers,
             const std::map<std::string, std::string> &peer_addrs)
        : id(node_id) {

      // 创建测试目录
      std::string test_dir = "./test_replication/" + id;
      system(("mkdir -p " + test_dir).c_str());

      node = std::make_unique<raft::RaftNode>(id, peers, test_dir);

      auto client = std::make_unique<raft::RaftRpcClient>();
      for (const auto &[peer_id, peer_addr] : peer_addrs) {
        if (peer_id != id) {
          client->AddPeer(peer_id, peer_addr);
        }
      }
      node->SetRpcClient(std::move(client));

      server = std::make_unique<raft::RaftRpcServer>(addr, node.get());
    }

    void Start() {
      server->Start();
      node->Start();
      is_running = true;
    }

    void Stop() {
      is_running = false;
      node->Stop();
      server->Stop();
    }
  };

  void SetUp() override {
    system("rm -rf ./test_replication");
    system("mkdir -p ./test_replication");
  }

  void TearDown() override {
    for (auto &node : nodes_) {
      if (node->is_running) {
        node->Stop();
      }
    }
    nodes_.clear();
  }

  void CreateCluster(int size) {
    std::map<std::string, std::string> addrs;
    std::vector<std::string> ids;

    for (int i = 1; i <= size; i++) {
      std::string id = "node" + std::to_string(i);
      std::string addr = "127.0.0.1:" + std::to_string(10000 + i);
      addrs[id] = addr;
      ids.push_back(id);
    }

    for (const auto &[id, addr] : addrs) {
      std::vector<std::string> peers;
      for (const auto &peer_id : ids) {
        if (peer_id != id) {
          peers.push_back(peer_id);
        }
      }

      nodes_.push_back(std::make_unique<TestNode>(id, addr, peers, addrs));
    }

    for (auto &node : nodes_) {
      node->Start();
    }

    // 等待选出Leader
    WaitForLeader();
  }

  raft::RaftNode *FindLeader() {
    for (auto &node : nodes_) {
      if (node->is_running && node->node->IsLeader()) {
        return node->node.get();
      }
    }
    return nullptr;
  }

  void WaitForLeader(int timeout_seconds = 5) {
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start <
           std::chrono::seconds(timeout_seconds)) {
      if (FindLeader() != nullptr) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    FAIL() << "No leader elected within timeout";
  }

  bool CheckLogsConsistent(uint64_t expected_last_index) {
    std::map<uint64_t, std::string> reference_logs;
    bool first = true;

    for (auto &node : nodes_) {
      if (!node->is_running)
        continue;

      auto last_index = node->node->GetLogManager()->GetLastIndex();
      if (last_index < expected_last_index) {
        return false; // 还没有复制完成
      }

      // 收集日志进行比较
      if (first) {
        // 使用第一个节点作为参考
        for (uint64_t i = 1; i <= expected_last_index; i++) {
          auto entry = node->node->GetLogManager()->GetEntry(i);
          reference_logs[i] = entry.command();
        }
        first = false;
      } else {
        // 比较其他节点的日志
        for (uint64_t i = 1; i <= expected_last_index; i++) {
          auto entry = node->node->GetLogManager()->GetEntry(i);
          if (entry.command() != reference_logs[i]) {
            printf("Log mismatch at index %llu: %s vs %s\n", i,
                   entry.command().c_str(), reference_logs[i].c_str());
            return false;
          }
        }
      }
    }

    return true;
  }

  std::vector<std::unique_ptr<TestNode>> nodes_;
};

TEST_F(LogReplicationTest, BasicReplication) {
  CreateCluster(3);

  auto *leader = FindLeader();
  ASSERT_NE(leader, nullptr);

  // 提交一些命令
  std::vector<std::future<bool>> futures;
  for (int i = 1; i <= 5; i++) {
    std::string cmd = "command" + std::to_string(i);
    futures.push_back(leader->SubmitCommand(cmd, "req" + std::to_string(i)));
  }

  // 等待命令提交
  for (auto &future : futures) {
    EXPECT_TRUE(future.get());
  }

  // 等待复制完成
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // 验证所有节点的日志一致
  EXPECT_TRUE(CheckLogsConsistent(5));

  // 验证提交索引
  for (auto &node : nodes_) {
    if (node->is_running) {
      EXPECT_GE(node->node->GetCommitIndex(), 5);
    }
  }
}

TEST_F(LogReplicationTest, ReplicationWithFailure) {
  CreateCluster(5);

  auto *leader = FindLeader();
  ASSERT_NE(leader, nullptr);

  // 提交一些命令
  for (int i = 1; i <= 3; i++) {
    leader->SubmitCommand("cmd" + std::to_string(i), "req" + std::to_string(i));
  }

  // 停止一个follower
  for (auto &node : nodes_) {
    if (node->is_running && !node->node->IsLeader()) {
      printf("Stopping follower %s\n", node->id.c_str());
      node->Stop();
      break;
    }
  }

  // 等待并获取新Leader（如果发生选举）
  WaitForLeader();
  leader = FindLeader();
  ASSERT_NE(leader, nullptr);

  // 继续提交命令
  for (int i = 4; i <= 6; i++) {
    leader->SubmitCommand("cmd" + std::to_string(i), "req" + std::to_string(i));
  }

  // 等待复制
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // 验证运行中的节点日志一致
  int running_count = 0;
  for (auto &node : nodes_) {
    if (node->is_running) {
      running_count++;
      // 1 (no-op) + 6 (commands) = 7
      EXPECT_EQ(node->node->GetLogManager()->GetLastIndex(), 7);
    }
  }
  EXPECT_EQ(running_count, 4); // 5个节点中有4个在运行
}

TEST_F(LogReplicationTest, CatchUpAfterReconnect) {
  CreateCluster(3);

  auto *leader = FindLeader();
  ASSERT_NE(leader, nullptr);

  // 找到一个follower
  TestNode *follower_node = nullptr;
  for (auto &node : nodes_) {
    if (node->is_running && !node->node->IsLeader()) {
      follower_node = node.get();
      break;
    }
  }
  ASSERT_NE(follower_node, nullptr);

  // 停止follower
  std::string follower_id = follower_node->id;
  follower_node->Stop();

  // Leader继续接收命令
  for (int i = 1; i <= 10; i++) {
    leader->SubmitCommand("cmd" + std::to_string(i), "req" + std::to_string(i));
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // 重启follower
  follower_node->Start();
  printf("Restarted follower %s\n", follower_id.c_str());

  // 等待追赶
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // 验证follower已经追上
  EXPECT_EQ(follower_node->node->GetLogManager()->GetLastIndex(),
            leader->GetLogManager()->GetLastIndex());
  EXPECT_GE(follower_node->node->GetCommitIndex(), leader->GetCommitIndex());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}