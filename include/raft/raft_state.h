// include/raft/raft_state.h
#pragma once

#include "log_manager.h"
#include "raft.pb.h"
#include "rpc/raft_rpc_client.h"
#include "storage/kv_state_machine.h"
#include "storage/rocksdb_storage.h"

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace raft {
using std::lock_guard, std::mutex;
using std::shared_ptr, std::unique_ptr;
using std::vector, std::string;
namespace chrono = std::chrono;

// 节点状态枚举
enum class NodeState {
  FOLLOWER,  // 跟随者
  CANDIDATE, // 候选人
  LEADER     // 领导者
};

// 将枚举转换为字符串（用于日志）
inline const char *StateToString(const NodeState &state) {
  switch (state) {
  case NodeState::FOLLOWER:
    return "FOLLOWER";
  case NodeState::CANDIDATE:
    return "CANDIDATE";
  case NodeState::LEADER:
    return "LEADER";
  }
  return "UNKNOWN";
}

// 持久化状态（需要持久化到磁盘）
struct PersistentState {
  uint64_t current_term = 0; // 服务器看到的最新任期
  string voted_for;          // 当前任期投票给哪个候选人
  vector<LogEntry> logs;     // 日志条目

  // 序列化和反序列化方法
  string Serialize() const;
  bool Deserialize(const string &data);
};

// 易失性状态（所有服务器）
struct VolatileState {
  uint64_t commit_index = 0; // 已知被提交的最高日志条目索引
  uint64_t last_applied = 0; // 已应用到状态机的最高日志条目索引
};

// 领导者状态（易失性，仅领导者使用）
struct LeaderState {
  vector<uint64_t> next_index;  // 每个服务器下一个要发送的日志条目索引
  vector<uint64_t> match_index; // 每个服务器已知复制的最高日志条目索引

  void Initialize(size_t peer_count, uint64_t last_log_index) {
    next_index.resize(peer_count, last_log_index + 1);
    match_index.resize(peer_count, 0);
  }
};

// 时间相关常量
struct RaftTiming {
  // 选举超时：150-300ms（可以根据网络情况调整）
  static constexpr auto ELECTION_TIMEOUT_MIN = std::chrono::milliseconds(150);
  static constexpr auto ELECTION_TIMEOUT_MAX = std::chrono::milliseconds(300);

  // 心跳间隔：50ms（应该远小于选举超时）
  static constexpr auto HEARTBEAT_INTERVAL = std::chrono::milliseconds(50);

  // RPC超时：30ms
  static constexpr auto RPC_TIMEOUT = std::chrono::milliseconds(30);
};

// Raft节点类
class RaftNode {
public:
  RaftNode(const string &node_id, const vector<string> &peers,
           const string &storage_path);
  ~RaftNode() { Stop(); }

  // 启动和停止
  void Start();
  void Stop();

  // 设置RPC客户端
  void SetRpcClient(unique_ptr<RaftRpcClient> client);

  // 获取状态(内部加锁)
  NodeState GetState() const;
  uint64_t GetCurrentTerm() const;
  string GetLeaderId() const;
  uint64_t GetCommitIndex() const; // 获取提交索引
  uint64_t GetLastApplied() const; // 获取应用索引
  bool IsLeader() const;

  // 获取状态(直接获取)
  string GetNodeId() const { return node_id_; }
  bool IsRunning() const { return running_; }
  size_t GetClusterSize() const { return peers_.size() + 1; }

  // 客户端请求接口
  struct ClientRequest {
    string request_id; // 客户端请求的唯一标识, 用于去重和异步通知客户端请求结果
    string command;    // 客户端请求的命令,例如put,get,delete
    std::promise<bool> promise; // 用于异步通知客户端请求结果
  };
  std::future<bool> SubmitCommand(const string &command,
                                  const string &request_id);

  // RPC处理方法（由RPC服务调用）
  void HandleRequestVote(const RequestVoteRequest *request,
                         RequestVoteReply *reply);
  void HandleAppendEntries(const AppendEntriesRequest *request,
                           AppendEntriesReply *reply);
  void HandleInstallSnapshot(const InstallSnapshotRequest *request,
                             InstallSnapshotReply *reply);

  // 获取状态机
  storage::KVStateMachine *GetStateMachine() const {
    return state_machine_.get();
  }

private:
  // 状态转换
  void BecomeFollower(uint64_t term);
  void BecomeCandidate();
  void BecomeLeader();

  // 选举相关
  void StartElection();
  void ResetElectionTimer();
  chrono::milliseconds GetRandomElectionTimeout();

  // 日志复制相关
  void ReplicateLog();       // 向所有follower复制日志
  void AdvanceCommitIndex(); // 推进提交索引
  void ApplyStateMachine();  // 应用状态机
  // 处理日志不一致
  void HandleLogInconsistency(size_t peer_index,
                              const AppendEntriesReply &reply);

  // 持久化
  void SaveState();
  void LoadState();

  // 后台线程
  void ElectionThread();  // 选举线程
  void HeartbeatThread(); // 发送心跳
  void ApplyThread();     // 应用状态机

  // 存储层方法
  void LoadFromStorage();
  void SaveToStorage();
  void MaybeCreateSnapshot();
  bool InstallSnapshotData(const string &snapshot_data,
                           uint64_t last_included_index,
                           uint64_t last_included_term);
  // 更新日志追加方法
  void AppendLogEntry(const LogEntry &entry);

public: // 测试接口
  // 强制触发选举（用于测试）
  void TriggerElection() {
    lock_guard<mutex> lock(state_mutex_);
    if (state_ != NodeState::LEADER) {
      // 立即超时
      last_heartbeat_time_ = chrono::steady_clock::now() - chrono::seconds(10);
    }
  }

  // 获取日志管理器（用于测试）
  LogManager *GetLogManager() const { return log_manager_.get(); }

  // 获取特定follower的next_index（用于调试）
  uint64_t GetNextIndex(const string &peer_id) const {
    lock_guard<mutex> lock(state_mutex_);
    if (state_ != NodeState::LEADER || !leader_state_) {
      return 0;
    }

    for (size_t i = 0; i < peers_.size(); i++) {
      if (peers_[i] == peer_id) {
        return leader_state_->next_index[i];
      }
    }
    return 0;
  }

private:
  // 基本信息
  const std::string node_id_;
  const std::vector<std::string> peers_;
  const std::string storage_path_;

  // 基本状态
  mutable mutex state_mutex_;
  NodeState state_ = NodeState::FOLLOWER;
  std::string current_leader_;

  // 持久化状态
  PersistentState persistent_state_;

  // 易失性状态
  VolatileState volatile_state_;

  // Leader状态
  unique_ptr<LeaderState> leader_state_;

  // 日志管理器
  unique_ptr<LogManager> log_manager_;

  // 计时器
  chrono::steady_clock::time_point last_heartbeat_time_;
  chrono::milliseconds election_timeout_;

  // 线程控制
  std::atomic<bool> running_{false};
  std::thread election_thread_;
  std::thread heartbeat_thread_;
  std::thread apply_thread_;

  // 客户端请求队列
  mutex request_mutex_;
  std::condition_variable request_cv_;
  // 客户端请求映射
  std::unordered_map<uint64_t, shared_ptr<ClientRequest>> pending_requests_;

  // RPC客户端
  unique_ptr<RaftRpcClient> rpc_client_;

  // 存储层
  unique_ptr<storage::RocksDBStorage> storage_;
  unique_ptr<storage::KVStateMachine> state_machine_;

  // 快照相关
  uint64_t snapshot_index_ = 0;
  uint64_t snapshot_term_ = 0;
  static constexpr size_t SNAPSHOT_THRESHOLD = 1000; // 每1000条日志创建快照
};

} // namespace raft