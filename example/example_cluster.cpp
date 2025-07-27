// example/example_cluster.cpp
#include <iostream>
#include <map>
#include <memory>
#include <signal.h>
#include <thread>
#include <vector>

#include "raft/raft_state.h"
#include "rpc/kv_client.h"
#include "rpc/kvstore_service.h"
#include "rpc/raft_rpc_client.h"
#include "rpc/raft_rpc_server.h"

std::atomic<bool> g_running{true};

void SignalHandler(int signal) {
  std::cout << "\nReceived signal " << signal << ", shutting down..."
            << std::endl;
  g_running = false;
}

class RaftKVNode {
public:
  RaftKVNode(const std::string &node_id, const std::string &raft_addr,
             const std::string &kv_addr, const std::vector<std::string> &peers,
             const std::map<std::string, std::string> &peer_addrs,
             const std::string &data_dir)
      : id_(node_id), raft_addr_(raft_addr), kv_addr_(kv_addr) {

    // 创建Raft节点
    raft_node_ = std::make_unique<raft::RaftNode>(id_, peers, data_dir);

    // 配置RPC客户端
    auto rpc_client = std::make_unique<raft::RaftRpcClient>();
    for (const auto &[peer_id, peer_addr] : peer_addrs) {
      if (peer_id != id_) {
        rpc_client->AddPeer(peer_id, peer_addr);
      }
    }
    raft_node_->SetRpcClient(std::move(rpc_client));

    // 创建服务器
    raft_server_ =
        std::make_unique<raft::RaftRpcServer>(raft_addr_, raft_node_.get());
    kv_server_ =
        std::make_unique<kvstore::KVStoreServer>(kv_addr_, raft_node_.get());
  }

  void Start() {
    std::cout << "Starting node " << id_ << std::endl;
    std::cout << "  Raft RPC: " << raft_addr_ << std::endl;
    std::cout << "  KV Service: " << kv_addr_ << std::endl;

    raft_server_->Start();
    kv_server_->Start();
    raft_node_->Start();
  }

  void Stop() {
    std::cout << "Stopping node " << id_ << std::endl;
    raft_node_->Stop();
    raft_server_->Stop();
    kv_server_->Stop();
  }

  bool IsLeader() const { return raft_node_->IsLeader(); }
  std::string GetNodeId() const { return id_; }
  std::string GetKVAddr() const { return kv_addr_; }

  void PrintStatus() {
    std::cout << id_ << ": " << raft::StateToString(raft_node_->GetState())
              << " (term=" << raft_node_->GetCurrentTerm() << ", "
              << "commit=" << raft_node_->GetCommitIndex() << ", "
              << "applied=" << raft_node_->GetLastApplied() << ")";

    if (IsLeader()) {
      std::cout << " [LEADER]";
    }

    auto stats = raft_node_->GetStateMachine()->GetStats();
    std::cout << " KV stats: puts=" << stats.put_count
              << ", gets=" << stats.get_count
              << ", deletes=" << stats.delete_count;

    std::cout << std::endl;
  }

private:
  std::string id_;
  std::string raft_addr_;
  std::string kv_addr_;

  std::unique_ptr<raft::RaftNode> raft_node_;
  std::unique_ptr<raft::RaftRpcServer> raft_server_;
  std::unique_ptr<kvstore::KVStoreServer> kv_server_;
};

void RunInteractiveClient(
    const std::vector<std::unique_ptr<RaftKVNode>> &nodes) {
  std::cout << "\n=== Interactive KV Client ===" << std::endl;
  std::cout
      << "Commands: put <key> <value>, get <key>, delete <key>, status, quit"
      << std::endl;

  // 找到leader的KV地址
  std::string leader_addr;
  for (const auto &node : nodes) {
    if (node->IsLeader()) {
      leader_addr = node->GetKVAddr();
      break;
    }
  }

  if (leader_addr.empty()) {
    std::cout << "No leader found!" << std::endl;
    return;
  }

  kvstore::KVClient client(leader_addr);

  std::string cmd;
  while (g_running && std::cin >> cmd) {
    if (cmd == "put") {
      std::string key, value;
      std::cin >> key >> value;

      auto start = std::chrono::high_resolution_clock::now();
      if (client.Put(key, value)) {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "OK (" << duration.count() << "ms)" << std::endl;
      } else {
        std::cout << "Failed: " << client.GetLastError() << std::endl;

        // 尝试重新连接到新leader
        if (!client.GetLeaderHint().empty()) {
          std::cout << "Redirecting to leader: " << client.GetLeaderHint()
                    << std::endl;
          // 这里可以实现自动重定向
        }
      }

    } else if (cmd == "get") {
      std::string key, value;
      std::cin >> key;

      auto start = std::chrono::high_resolution_clock::now();
      if (client.Get(key, &value)) {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << value << " (" << duration.count() << "ms)" << std::endl;
      } else {
        std::cout << "Failed: " << client.GetLastError() << std::endl;
      }

    } else if (cmd == "delete") {
      std::string key;
      std::cin >> key;

      if (client.Delete(key)) {
        std::cout << "OK" << std::endl;
      } else {
        std::cout << "Failed: " << client.GetLastError() << std::endl;
      }

    } else if (cmd == "status") {
      std::cout << "\n--- Cluster Status ---" << std::endl;
      for (const auto &node : nodes) {
        node->PrintStatus();
      }
      std::cout << "-------------------\n" << std::endl;

    } else if (cmd == "quit") {
      break;

    } else {
      std::cout << "Unknown command: " << cmd << std::endl;
    }
  }
}

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " <cluster_size>" << std::endl;
    std::cout << "Example: " << argv[0] << " 3" << std::endl;
    return 1;
  }

  int cluster_size = std::stoi(argv[1]);
  if (cluster_size < 1 || cluster_size > 9) {
    std::cout << "Cluster size must be between 1 and 9" << std::endl;
    return 1;
  }

  // 设置信号处理
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  // 配置集群
  std::map<std::string, std::string> raft_addrs;
  std::map<std::string, std::string> kv_addrs;
  std::vector<std::string> node_ids;

  for (int i = 1; i <= cluster_size; i++) {
    std::string id = "node" + std::to_string(i);
    node_ids.push_back(id);
    raft_addrs[id] = "127.0.0.1:" + std::to_string(40000 + i);
    kv_addrs[id] = "127.0.0.1:" + std::to_string(41000 + i);
  }

  // 创建节点
  std::vector<std::unique_ptr<RaftKVNode>> nodes;

  for (const auto &id : node_ids) {
    std::vector<std::string> peers;
    for (const auto &peer_id : node_ids) {
      if (peer_id != id) {
        peers.push_back(peer_id);
      }
    }

    std::string data_dir = "./cluster_data/" + id;
    system(("mkdir -p " + data_dir).c_str());

    nodes.push_back(std::make_unique<RaftKVNode>(
        id, raft_addrs[id], kv_addrs[id], peers, raft_addrs, data_dir));
  }

  // 启动所有节点
  std::cout << "\n=== Starting " << cluster_size
            << "-node Raft KV cluster ===" << std::endl;
  for (auto &node : nodes) {
    node->Start();
  }

  // 等待选举完成
  std::cout << "\nWaiting for leader election..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // 显示集群状态
  std::cout << "\n--- Initial Cluster Status ---" << std::endl;
  for (const auto &node : nodes) {
    node->PrintStatus();
  }
  std::cout << "----------------------------\n" << std::endl;

  // 运行交互式客户端
  RunInteractiveClient(nodes);

  // 关闭集群
  std::cout << "\nShutting down cluster..." << std::endl;
  for (auto &node : nodes) {
    node->Stop();
  }

  return 0;
}