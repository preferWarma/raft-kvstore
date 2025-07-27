// src/raft/raft_state.cpp
#include "raft/raft_state.h"
#include "lyf/LogSystem.h"
#include "raft/log_manager.h"
#include "rpc/raft_rpc_client.h"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <future>
#include <random>
#include <sstream>
#include <stdexcept>

namespace raft {

// PersistentState序列化实现
string PersistentState::Serialize() const {
  // 简单的文本序列化格式
  std::stringstream ss;
  ss << current_term << "\n";
  ss << voted_for << "\n";
  ss << logs.size() << "\n";

  for (const auto &entry : logs) {
    ss << entry.index() << " " << entry.term() << " " << entry.type() << " "
       << entry.command().size() << "\n";
    ss.write(entry.command().data(), entry.command().size());
    ss << "\n";
  }

  return ss.str();
}

bool PersistentState::Deserialize(const string &data) {
  std::stringstream ss(data);

  ss >> current_term;
  ss.ignore(); // 忽略换行符

  std::getline(ss, voted_for);

  size_t log_size;
  ss >> log_size;
  ss.ignore();

  logs.clear();
  logs.reserve(log_size);

  for (size_t i = 0; i < log_size; ++i) {
    LogEntry entry;
    uint64_t index, term;
    int type;
    size_t command_size;

    ss >> index >> term >> type >> command_size;
    ss.ignore();

    entry.set_index(index);
    entry.set_term(term);
    entry.set_type(static_cast<LogEntry::LogType>(type));

    if (command_size > 0) {
      string command(command_size, '\0');
      ss.read(&command[0], command_size);
      entry.set_command(std::move(command));
    }
    ss.ignore();

    logs.push_back(std::move(entry));
  }

  return true;
}

// RaftNode构造函数
RaftNode::RaftNode(const string &node_id, const vector<string> &peers,
                   const string &storage_path)
    : node_id_(node_id), peers_(peers), storage_path_(storage_path),
      log_manager_(std::make_unique<LogManager>()) {
  // 确保目录存在
  std::filesystem::create_directories(storage_path_);

  // 初始化rocksdb
  storage_ = std::make_unique<storage::RocksDBStorage>(storage_path_);
  if (!storage_->Open()) {
    LOG_FATAL("Failed to open RocksDB");
    throw std::runtime_error("Failed to open RocksDB");
  }
  // 初始化状态机
  state_machine_ = std::make_unique<storage::KVStateMachine>(storage_.get());

  // 加载持久化状态
  LoadState();

  // 初始化选举超时
  election_timeout_ = GetRandomElectionTimeout();
  last_heartbeat_time_ = chrono::steady_clock::now();
}

void RaftNode::Start() {
  running_ = true;

  // 启动后台线程
  election_thread_ = std::thread(&RaftNode::ElectionThread, this);
  heartbeat_thread_ = std::thread(&RaftNode::HeartbeatThread, this);
  apply_thread_ = std::thread(&RaftNode::ApplyThread, this);

  LOG_INFO("Raft node[{}] started\n", node_id_);
}

void RaftNode::Stop() {
  running_ = false;

  // 通知所有线程退出
  request_cv_.notify_all();

  // 等待线程结束
  if (election_thread_.joinable())
    election_thread_.join();
  if (heartbeat_thread_.joinable())
    heartbeat_thread_.join();
  if (apply_thread_.joinable())
    apply_thread_.join();

  // 保存状态
  SaveState();

  LOG_INFO("Raft node[{}] stopped\n", node_id_);
}

NodeState RaftNode::GetState() const {
  lock_guard<mutex> lock(state_mutex_);
  return state_;
}

uint64_t RaftNode::GetCurrentTerm() const {
  lock_guard<mutex> lock(state_mutex_);
  return persistent_state_.current_term;
}

string RaftNode::GetLeaderId() const {
  lock_guard<mutex> lock(state_mutex_);
  return current_leader_;
}

uint64_t RaftNode::GetCommitIndex() const {
  lock_guard<mutex> lock(state_mutex_);
  return volatile_state_.commit_index;
}

uint64_t RaftNode::GetLastApplied() const {
  lock_guard<mutex> lock(state_mutex_);
  return volatile_state_.last_applied;
}

bool RaftNode::IsLeader() const {
  lock_guard<mutex> lock(state_mutex_);
  return state_ == NodeState::LEADER;
}

void RaftNode::BecomeFollower(uint64_t term) {
  LOG_INFO("{} Becoming FOLLOWER for term {}", node_id_, term);

  state_ = NodeState::FOLLOWER;
  persistent_state_.current_term = term;
  persistent_state_.voted_for.clear();
  current_leader_.clear();

  // 重置选举计时器
  ResetElectionTimer();

  // 保存状态
  SaveState();
}

void RaftNode::BecomeCandidate() {
  LOG_INFO("{} Becoming CANDIDATE for term {}", node_id_,
           persistent_state_.current_term + 1);

  state_ = NodeState::CANDIDATE;
  persistent_state_.current_term++;
  persistent_state_.voted_for = node_id_;
  current_leader_.clear();

  // 重置选举计时器
  ResetElectionTimer();

  // 保存状态
  SaveState();
}

void RaftNode::BecomeLeader() {
  LOG_INFO("{} Becoming LEADER for term {}", node_id_,
           persistent_state_.current_term);

  state_ = NodeState::LEADER;
  current_leader_ = node_id_;

  // 初始化leader状态
  leader_state_ = std::make_unique<LeaderState>();
  uint64_t last_log_index = log_manager_->GetLastIndex();
  leader_state_->Initialize(peers_.size(), last_log_index);

  // 添加一个no-op条目来确立领导地位
  LogEntry noop_entry;
  noop_entry.set_index(last_log_index + 1);
  noop_entry.set_term(persistent_state_.current_term);
  noop_entry.set_type(LogEntry::NOOP);
  noop_entry.set_command(""); // 空命令

  log_manager_->Append(noop_entry);

  LOG_DEBUG("{} Added no-op entry at index {}", node_id_, noop_entry.index());

  // 立即发送心跳，宣告领导地位
  ReplicateLog();

  // 通知等待的客户端请求
  request_cv_.notify_all();
}

void RaftNode::ResetElectionTimer() {
  last_heartbeat_time_ = chrono::steady_clock::now();
  election_timeout_ = GetRandomElectionTimeout();
}

chrono::milliseconds RaftNode::GetRandomElectionTimeout() {
  static thread_local std::random_device rd;
  static thread_local std::mt19937 gen(rd());

  // 使用更大的范围来减少split vote的概率
  int min_ms = RaftTiming::ELECTION_TIMEOUT_MIN.count();
  int max_ms = RaftTiming::ELECTION_TIMEOUT_MAX.count();

  // 确保有足够的随机性
  std::uniform_int_distribution<int> dist(min_ms, max_ms);

  auto timeout = chrono::milliseconds(dist(gen));

  return timeout;
}

void RaftNode::ElectionThread() {
  LOG_INFO("{} Election thread started", node_id_);

  while (running_) {
    // 检查是否需要开始选举
    bool should_start_election = false;
    {
      lock_guard<mutex> lock(state_mutex_);

      // 只有follower和candidate会触发选举超时
      if (state_ != NodeState::LEADER) {
        auto now = chrono::steady_clock::now();
        auto elapsed = chrono::duration_cast<chrono::milliseconds>(
            now - last_heartbeat_time_);

        if (elapsed >= election_timeout_) {
          should_start_election = true;
          LOG_DEBUG("{} Election timeout! Elapsed: {}ms >= timeout: {}ms",
                    node_id_, elapsed.count(), election_timeout_.count());
        }
      }
    }

    if (should_start_election) {
      StartElection();
    }

    // 短暂休眠，避免忙等待
    std::this_thread::sleep_for(chrono::milliseconds(10));
  }

  LOG_INFO("{} Election thread stopped", node_id_);
}

void RaftNode::HeartbeatThread() {
  while (running_) {
    {
      lock_guard<mutex> lock(state_mutex_);
      if (state_ == NodeState::LEADER) {
        ReplicateLog();
      }
    }

    std::this_thread::sleep_for(RaftTiming::HEARTBEAT_INTERVAL);
  }
}

void RaftNode::ApplyThread() {
  while (running_) {
    std::unique_lock<std::mutex> lock(state_mutex_);

    // 等待有新的提交
    request_cv_.wait(lock, [this] {
      return !running_ ||
             volatile_state_.last_applied < volatile_state_.commit_index;
    });

    if (!running_) {
      break;
    }

    // 应用所有已提交但未应用的日志
    while (volatile_state_.last_applied < volatile_state_.commit_index) {
      volatile_state_.last_applied++;

      auto entry = log_manager_->GetEntry(volatile_state_.last_applied);

      if (entry.type() == LogEntry::NORMAL) {
        // 应用到状态机
        storage::ApplyResult result = state_machine_->Apply(entry.command());

        LOG_DEBUG("{} Applied log entry {}", node_id_,
                  volatile_state_.last_applied);

        // 如果是Leader，通知等待的客户端
        if (state_ == NodeState::LEADER) {
          auto it = pending_requests_.find(volatile_state_.last_applied);
          if (it != pending_requests_.end()) {
            it->second->promise.set_value(result.success);
            pending_requests_.erase(it);
          }
        }
      }
    }
  }
}

void RaftNode::SaveState() {
  string filename = storage_path_ + "/raft_state.dat";
  std::ofstream file(filename, std::ios::binary);
  if (file.is_open()) {
    string data = persistent_state_.Serialize();
    file.write(data.data(), data.size());
    file.close();
  }
}

void RaftNode::LoadState() {
  string filename = storage_path_ + "/raft_state.dat";
  std::ifstream file(filename, std::ios::binary);
  if (file.is_open()) {
    string data((std::istreambuf_iterator<char>(file)),
                std::istreambuf_iterator<char>());
    persistent_state_.Deserialize(data);
    file.close();

    // 将日志恢复到LogManager
    log_manager_->RestoreFromPersistentState(persistent_state_.logs);
  }
}

// RPC处理方法的骨架实现
void RaftNode::HandleRequestVote(const RequestVoteRequest *request,
                                 RequestVoteReply *reply) {
  lock_guard<mutex> lock(state_mutex_);

  // 设置当前任期
  reply->set_term(persistent_state_.current_term);
  reply->set_vote_granted(false);

  LOG_DEBUG("{} Received RequestVote from {} for term {} (my term: {})",
            node_id_, request->candidate_id(), request->term(),
            persistent_state_.current_term);

  // 规则1: 如果请求的任期小于当前任期，拒绝投票
  if (request->term() < persistent_state_.current_term) {
    LOG_DEBUG("{} Rejecting vote: request term {} < current term {}", node_id_,
              request->term(), persistent_state_.current_term);
    return;
  }

  // 规则2: 如果请求的任期大于当前任期，更新任期并转为follower
  if (request->term() > persistent_state_.current_term) {
    LOG_DEBUG("{} Higher term detected ({}) > current term ({}), updating term",
              node_id_, request->term(), persistent_state_.current_term);

    BecomeFollower(request->term());
    // 注意：BecomeFollower会清空voted_for，所以可以继续投票
  }

  // 规则3: 检查是否已经投票
  bool can_vote = persistent_state_.voted_for.empty() ||
                  persistent_state_.voted_for == request->candidate_id();

  if (!can_vote) {
    LOG_DEBUG("{} Already voted for {} in term {}", node_id_,
              persistent_state_.voted_for, persistent_state_.current_term);
    return;
  }

  // 规则4: 检查候选人的日志是否至少和自己一样新
  uint64_t my_last_log_index = log_manager_->GetLastIndex();
  uint64_t my_last_log_term = log_manager_->GetLastTerm();

  bool log_is_up_to_date = (request->last_log_term() > my_last_log_term) ||
                           (request->last_log_term() == my_last_log_term &&
                            request->last_log_index() >= my_last_log_index);

  if (!log_is_up_to_date) {
    LOG_DEBUG("{} Rejecting vote: candidate's log not up-to-date "
              "(candidate: term={} index={}, mine: term={} index={})",
              node_id_, request->last_log_term(), request->last_log_index(),
              my_last_log_term, my_last_log_index);
    return;
  }

  // 投票给候选人
  persistent_state_.voted_for = request->candidate_id();
  reply->set_vote_granted(true);
  reply->set_term(persistent_state_.current_term);

  // 重置选举计时器
  ResetElectionTimer();

  // 持久化状态
  SaveState();

  LOG_DEBUG("{} Voted for {} in term {}", node_id_, request->candidate_id(),
            persistent_state_.current_term);
}

void RaftNode::HandleAppendEntries(const AppendEntriesRequest *request,
                                   AppendEntriesReply *reply) {
  lock_guard<mutex> lock(state_mutex_);

  reply->set_term(persistent_state_.current_term);
  reply->set_success(false);

  // 检查节点是否在运行
  if (!running_) {
    return;
  }

  // 打印接收信息（除了心跳）
  if (request->entries_size() > 0) {
    LOG_DEBUG("{} Received AppendEntries from {}: term={}, prev_idx={}, "
              "prev_term={}, entries={}, commit={}",
              node_id_, request->leader_id(), request->term(),
              request->prev_log_index(), request->prev_log_term(),
              request->entries_size(), request->leader_commit());
  }

  // 1. 如果term < currentTerm，拒绝
  if (request->term() < persistent_state_.current_term) {
    LOG_DEBUG("{} Rejecting AppendEntries: outdated term {} < {}", node_id_,
              request->term(), persistent_state_.current_term);
    return;
  }

  // 2. 如果term >= currentTerm，识别leader
  bool need_persist = false;

  if (request->term() > persistent_state_.current_term) {
    LOG_DEBUG("{} Higher term {} detected, updating from {}", node_id_,
              request->term(), persistent_state_.current_term);
    persistent_state_.current_term = request->term();
    persistent_state_.voted_for.clear();
    need_persist = true;
  }

  // 转为follower（如果不是）
  if (state_ != NodeState::FOLLOWER) {
    state_ = NodeState::FOLLOWER;
    leader_state_.reset();
  }

  // 更新leader信息
  current_leader_ = request->leader_id();
  ResetElectionTimer();

  // 3. 检查日志一致性
  bool log_ok = true;

  if (request->prev_log_index() > 0) {
    if (request->prev_log_index() > log_manager_->GetLastIndex()) {
      // 本地日志太短
      log_ok = false;
      reply->set_conflict_index(log_manager_->GetLastIndex() + 1);
      reply->set_conflict_term(0);

      LOG_DEBUG("{} Log too short: my last={}, required prev={}", node_id_,
                log_manager_->GetLastIndex(), request->prev_log_index());
    } else {
      // 检查prev_log位置的任期是否匹配
      uint64_t local_prev_term =
          log_manager_->GetTermOfIndex(request->prev_log_index());
      if (local_prev_term != request->prev_log_term()) {
        log_ok = false;

        // 找到冲突任期的第一个索引，帮助leader快速回退
        uint64_t conflict_term = local_prev_term;
        uint64_t conflict_index = request->prev_log_index();

        // 向前查找该任期的第一个条目
        while (conflict_index > 1 && log_manager_->GetTermOfIndex(
                                         conflict_index - 1) == conflict_term) {
          conflict_index--;
        }

        reply->set_conflict_index(conflict_index);
        reply->set_conflict_term(conflict_term);

        LOG_DEBUG("{} Log conflict at index {}: my term={}, leader term={}",
                  node_id_, request->prev_log_index(), local_prev_term,
                  request->prev_log_term());
      }
    }
  }

  if (!log_ok) {
    if (need_persist) {
      SaveState();
    }
    return;
  }

  // 4. 追加新日志条目
  if (request->entries_size() > 0) {
    vector<LogEntry> new_entries;
    for (const auto &entry : request->entries()) {
      new_entries.push_back(entry);
    }

    // 删除冲突的日志并追加新日志
    log_manager_->AppendEntries(request->prev_log_index(),
                                request->prev_log_term(), new_entries);
    need_persist = true;

    LOG_DEBUG("{} Appended {} log entries, last index now {}", node_id_,
              request->entries_size(), log_manager_->GetLastIndex());
  }

  // 5. 更新commit index
  if (request->leader_commit() > volatile_state_.commit_index) {
    uint64_t new_commit =
        std::min(request->leader_commit(), log_manager_->GetLastIndex());
    if (new_commit > volatile_state_.commit_index) {
      volatile_state_.commit_index = new_commit;
      LOG_DEBUG("{} Updated commit index to {}", node_id_,
                volatile_state_.commit_index);

      // 通知应用线程
      request_cv_.notify_all();
    }
  }

  // 6. 持久化状态（如果有变化）
  if (need_persist) {
    SaveState();
  }

  // 7. 返回成功
  reply->set_success(true);
  reply->set_term(persistent_state_.current_term);
}

void RaftNode::HandleInstallSnapshot(const InstallSnapshotRequest *request,
                                     InstallSnapshotReply *reply) {
  lock_guard<mutex> lock(state_mutex_);

  reply->set_term(persistent_state_.current_term);

  // 如果请求的任期小于当前任期，拒绝
  if (request->term() < persistent_state_.current_term) {
    return;
  }

  // 重置选举计时器
  ResetElectionTimer();

  // 如果请求的任期大于当前任期，转为follower
  if (request->term() > persistent_state_.current_term) {
    BecomeFollower(request->term());
  }

  // 如果快照的 last_included_index 小于或等于当前节点的
  // commit_index，说明是过时的快照
  if (request->last_included_index() <= volatile_state_.commit_index) {
    LOG_WARN(
        "Node {} received a old snapshot, index {} <= commit_index {},ignoring",
        node_id_, request->last_included_index(), volatile_state_.commit_index);
    return;
  }

  // 当前实现假定快照一次性发送完毕
  if (!request->done()) {
    LOG_WARN("Node {} received a snapshot chunk, but chunking is not "
             "implemented yet",
             node_id_);
    return;
  }

  // 1. 让状态机从快照数据中恢复
  if (!state_machine_->RestoreSnapshot(request->data())) {
    LOG_WARN("Node {} failed to restore state machine from snapshot.",
             node_id_);
    return;
  }

  // 2. 更新日志管理器以反映快照
  // 日志将被截断，旧的日志条目被快照取代
  vector<LogEntry> empty_log;
  log_manager_->InstallSnapshot(request->last_included_index(),
                                request->last_included_term(), empty_log);

  // 3. 更新节点的易失性状态
  volatile_state_.commit_index = request->last_included_index();
  volatile_state_.last_applied = request->last_included_index();

  // 4. 更新持久化状态
  // 持久化日志也需要反映快照，旧日志被清空
  persistent_state_.logs = log_manager_->GetAllEntries();
  SaveState();

  LOG_INFO("Node {} successfully installed snapshot up to index {}", node_id_,
           request->last_included_index());
}

void RaftNode::SetRpcClient(std::unique_ptr<RaftRpcClient> client) {
  rpc_client_ = std::move(client);
}

void RaftNode::StartElection() {
  // 检查是否有RPC客户端
  if (!rpc_client_) {
    LOG_FATAL("Node {} has no RPC client configured, cannot start election",
              node_id_);
    return;
  }

  // 准备选举
  RequestVoteRequest request;
  {
    lock_guard<mutex> lock(state_mutex_);

    // 转为候选人状态
    BecomeCandidate();

    // 填充请求
    request.set_term(persistent_state_.current_term);
    request.set_candidate_id(node_id_);
    request.set_last_log_index(log_manager_->GetLastIndex());
    request.set_last_log_term(log_manager_->GetLastTerm());

    LOG_DEBUG("Node {} starting election for term {} (last_log_index={}, "
              "last_log_term={})",
              node_id_, persistent_state_.current_term,
              request.last_log_index(), request.last_log_term());
  }

  // 统计票数（已经投给自己一票）
  std::atomic<int> votes_received(1);
  std::atomic<int> responses_received(0);
  int majority = (peers_.size() + 1) / 2 + 1;

  // 为每个peer创建一个线程来请求投票
  vector<std::thread> vote_threads;

  for (const auto &peer : peers_) {
    vote_threads.emplace_back([this, peer, request, &votes_received,
                               &responses_received, majority]() {
      RequestVoteReply reply;
      bool success = rpc_client_->SendRequestVote(peer, request, &reply,
                                                  chrono::milliseconds(100));

      responses_received++;

      if (success) {
        lock_guard<mutex> lock(state_mutex_);

        // 检查是否仍然是候选人
        if (state_ != NodeState::CANDIDATE ||
            persistent_state_.current_term != request.term()) {
          return;
        }

        // 如果发现更高的任期，转为follower
        if (reply.term() > persistent_state_.current_term) {
          LOG_DEBUG(
              "Node {} received higher term {} from {}, becoming follower",
              node_id_, reply.term(), peer);
          BecomeFollower(reply.term());
          return;
        }

        // 统计投票
        if (reply.vote_granted() &&
            reply.term() == persistent_state_.current_term) {
          int current_votes = ++votes_received;
          LOG_DEBUG("Node {} received vote from {}, total votes: {}/{}",
                    node_id_, peer, current_votes, majority);

          // 检查是否获得多数票
          if (current_votes >= majority && state_ == NodeState::CANDIDATE) {
            LOG_DEBUG("Node {} won election with {} votes", node_id_,
                      current_votes);
            BecomeLeader();
          }
        }
      } else {
        LOG_WARN("Node {} failed to get vote from {} (RPC failed)", node_id_,
                 peer);
      }
    });
  }

  // 等待所有投票线程完成
  for (auto &t : vote_threads) {
    if (t.joinable()) {
      t.join();
    }
  }

  // 检查最终状态
  {
    lock_guard<mutex> lock(state_mutex_);
    if (state_ == NodeState::CANDIDATE) {
      LOG_DEBUG(
          "Node {} election failed, received {}/{} votes, {}/{} responses",
          node_id_, votes_received.load(), majority, responses_received.load(),
          peers_.size());
      // 保持候选人状态，等待下次选举超时
    }
  }
}

void RaftNode::ReplicateLog() {
  if (!rpc_client_ || state_ != NodeState::LEADER) {
    return;
  }

  // 为每个follower发送日志
  for (size_t i = 0; i < peers_.size(); i++) {
    const auto &peer = peers_[i];

    // 在单独的线程中处理每个peer，避免阻塞
    std::thread([this, peer, i]() {
      // 检查节点是否仍在运行且是Leader
      if (!running_ || state_ != NodeState::LEADER) {
        return;
      }

      // 准备AppendEntries请求
      AppendEntriesRequest request;
      {
        lock_guard<mutex> lock(state_mutex_);

        // 再次检查状态
        if (state_ != NodeState::LEADER) {
          return;
        }

        uint64_t next_idx = leader_state_->next_index[i];
        uint64_t prev_idx = next_idx - 1;

        request.set_term(persistent_state_.current_term);
        request.set_leader_id(node_id_);
        request.set_prev_log_index(prev_idx);
        request.set_prev_log_term(log_manager_->GetTermOfIndex(prev_idx));
        request.set_leader_commit(volatile_state_.commit_index);

        // 获取需要发送的日志条目
        uint64_t last_log_index = log_manager_->GetLastIndex();
        if (next_idx <= last_log_index) {
          auto entries = log_manager_->GetEntries(next_idx, last_log_index + 1);
          for (const auto &entry : entries) {
            *request.add_entries() = entry;
          }

          LOG_DEBUG(
              "Node {} sending {} entries to {} (next_idx={}, last_idx={})",
              node_id_, entries.size(), peer, next_idx, last_log_index);
        }
      }

      // 发送RPC（在锁外进行网络调用）
      AppendEntriesReply reply;
      bool success = rpc_client_->SendAppendEntries(peer, request, &reply,
                                                    chrono::milliseconds(100));

      // 处理响应
      if (success) {
        lock_guard<mutex> lock(state_mutex_);

        // 检查是否仍然是Leader
        if (state_ != NodeState::LEADER ||
            persistent_state_.current_term != request.term()) {
          return;
        }

        // 如果收到更高的任期，转为Follower
        if (reply.term() > persistent_state_.current_term) {
          LOG_DEBUG("Node {} received higher term {} from {}, stepping down",
                    node_id_, reply.term(), peer);
          BecomeFollower(reply.term());
          return;
        }

        if (reply.success()) {
          // 更新next_index和match_index
          uint64_t prev_log_index = request.prev_log_index();
          uint64_t entries_count = request.entries_size();

          leader_state_->next_index[i] = prev_log_index + entries_count + 1;
          leader_state_->match_index[i] = prev_log_index + entries_count;

          if (entries_count > 0) {
            LOG_DEBUG("Node {} successfully replicated to {}, match_index={}",
                      node_id_, peer, leader_state_->match_index[i]);
          }

          // 尝试推进commit index
          AdvanceCommitIndex();
        } else {
          // 日志不一致，需要回退
          HandleLogInconsistency(i, reply);
        }
      } else {
        // RPC失败，下次重试
        LOG_WARN("Node {} AppendEntries RPC to {} failed", node_id_, peer);
      }
    }).detach();
  }
}

std::future<bool> RaftNode::SubmitCommand(const string &command,
                                          const string &request_id) {
  auto request = std::make_shared<ClientRequest>();
  request->request_id = request_id;
  request->command = command;

  auto future = request->promise.get_future();

  {
    lock_guard<mutex> lock(state_mutex_);

    // 只有Leader可以处理客户端请求
    if (state_ != NodeState::LEADER) {
      request->promise.set_value(false);
      LOG_DEBUG(
          "Node {} rejecting client request: not leader (current leader: {})",
          node_id_, current_leader_);
      return future;
    }

    // 创建新的日志条目
    LogEntry entry;
    entry.set_index(log_manager_->GetLastIndex() + 1);
    entry.set_term(persistent_state_.current_term);
    entry.set_type(LogEntry::NORMAL);
    entry.set_command(command);

    // 追加并持久化
    AppendLogEntry(entry);

    // 保存请求，等待提交
    pending_requests_[entry.index()] = request;

    LOG_INFO("Node {} accepted client command at index {}: {}", node_id_,
             entry.index(), request_id);
  }

  // 立即开始复制
  ReplicateLog();

  return future;
}

// 应用已提交的日志
void RaftNode::ApplyStateMachine() {
  while (running_) {
    std::unique_lock<std::mutex> lock(state_mutex_);

    // 等待有新的提交
    request_cv_.wait(lock, [this] {
      return !running_ ||
             volatile_state_.last_applied < volatile_state_.commit_index;
    });

    if (!running_) {
      break;
    }

    // 应用所有已提交但未应用的日志
    while (volatile_state_.last_applied < volatile_state_.commit_index) {
      volatile_state_.last_applied++;

      auto entry = log_manager_->GetEntry(volatile_state_.last_applied);

      if (entry.type() == LogEntry::NORMAL) {
        // 实际应用到状态机
        auto result = state_machine_->Apply(entry.command());
        LOG_INFO("Node {} applied log entry {} to state machine: {}", node_id_,
                 volatile_state_.last_applied,
                 result.success ? "SUCCESS" : result.error.c_str());

        // 如果是Leader，通知等待的客户端
        if (state_ == NodeState::LEADER) {
          auto it = pending_requests_.find(volatile_state_.last_applied);
          if (it != pending_requests_.end()) {
            it->second->promise.set_value(true);
            pending_requests_.erase(it);
          }
        }
      }
    }
    // 检查是否需要创建快照
    MaybeCreateSnapshot();
  }
}

void RaftNode::AdvanceCommitIndex() {
  if (state_ != NodeState::LEADER) {
    return;
  }

  // 收集所有节点（包括自己）的match index
  vector<uint64_t> match_indices;
  match_indices.reserve(peers_.size() + 1);

  // 添加自己的日志索引
  match_indices.push_back(log_manager_->GetLastIndex());

  // 添加所有follower的match index
  for (size_t i = 0; i < leader_state_->match_index.size(); i++) {
    match_indices.push_back(leader_state_->match_index[i]);
  }

  // 排序找到中位数（多数派达成一致的最高索引）
  std::sort(match_indices.begin(), match_indices.end(),
            std::greater<uint64_t>());

  // 多数派索引（包括leader自己）
  size_t majority_index = peers_.size() / 2; // 因为包括了leader，所以是 N/2
  uint64_t new_commit_index = match_indices[majority_index];

  // 只能提交当前任期的日志
  if (new_commit_index > volatile_state_.commit_index) {
    // 从当前commit_index + 1开始检查
    for (uint64_t idx = volatile_state_.commit_index + 1;
         idx <= new_commit_index; idx++) {
      if (log_manager_->GetTermOfIndex(idx) == persistent_state_.current_term) {
        // 找到当前任期的日志，可以安全提交到这里
        volatile_state_.commit_index = idx;
        LOG_DEBUG("Node {} advanced commit index to {}", node_id_,
                  volatile_state_.commit_index);

        // 通知应用线程
        request_cv_.notify_all();
      } else if (log_manager_->GetTermOfIndex(idx) <
                 persistent_state_.current_term) {
        // 继续检查下一个
        continue;
      } else {
        // 不应该有更高任期的日志
        break;
      }
    }
  }
}

// 处理日志不一致的情况
void RaftNode::HandleLogInconsistency(size_t peer_index,
                                      const AppendEntriesReply &reply) {
  // 如果因为日志不一致而失败，减少next_index并重试
  // 这是简化的回退策略，每次只回退一个位置
  if (leader_state_->next_index[peer_index] > 1) {
    leader_state_->next_index[peer_index]--;
  }

  LOG_DEBUG(
      "Node {} log inconsistency with peer {}, decrementing next_index to {}",
      node_id_, peer_index, leader_state_->next_index[peer_index]);
}

void RaftNode::LoadFromStorage() {
  // 加载Raft状态
  storage_->LoadRaftState(&persistent_state_.current_term,
                          &persistent_state_.voted_for);

  // 加载快照（如果有）
  string snapshot_data;
  if (storage_->LoadSnapshot(&snapshot_index_, &snapshot_term_,
                             &snapshot_data)) {
    // 恢复状态机
    state_machine_->RestoreSnapshot(snapshot_data);

    // 设置日志管理器的快照信息
    log_manager_->InstallSnapshot(snapshot_index_, snapshot_term_, {});

    // 更新已应用索引
    volatile_state_.last_applied = snapshot_index_;

    LOG_INFO("Node {} loaded snapshot at index {}, term {}", node_id_,
             snapshot_index_, snapshot_term_);
  }

  // 加载日志
  uint64_t start_index = snapshot_index_ + 1;
  uint64_t last_index = 0;
  storage_->GetLastLogIndex(&last_index);

  if (last_index >= start_index) {
    vector<raft::LogEntry> entries;
    storage_->GetLogs(start_index, last_index + 1, &entries);

    for (const auto &entry : entries) {
      log_manager_->Append(entry);
    }

    LOG_INFO("Node {} loaded {} log entries from storage", node_id_,
             entries.size());
  }
}

void RaftNode::SaveToStorage() {
  // 保存Raft状态
  if (storage_->SaveRaftState(persistent_state_.current_term,
                              persistent_state_.voted_for)) {
    LOG_INFO("Node {} saved Raft state: term {}, voted_for {}", node_id_,
             persistent_state_.current_term, persistent_state_.voted_for);
  }
}

// 更新日志追加方法
void RaftNode::AppendLogEntry(const LogEntry &entry) {
  // 追加到内存
  log_manager_->Append(entry);

  // 持久化到RocksDB
  if (!storage_->AppendLog(entry)) {
    LOG_ERROR("Node {} failed to persist log entry {}", node_id_,
              entry.index());
  }
}

void RaftNode::MaybeCreateSnapshot() {
  // 检查是否需要创建快照
  if (volatile_state_.last_applied - snapshot_index_ < SNAPSHOT_THRESHOLD) {
    return;
  }

  LOG_INFO("Node {} creating snapshot at index {}", node_id_,
           volatile_state_.last_applied);

  // 创建快照
  string snapshot_data = state_machine_->TakeSnapshot();
  if (snapshot_data.empty()) {
    LOG_ERROR("Node {} failed to create snapshot", node_id_);
    return;
  }

  // 保存快照
  uint64_t snapshot_index = volatile_state_.last_applied;
  uint64_t snapshot_term = log_manager_->GetTermOfIndex(snapshot_index);

  if (storage_->SaveSnapshot(snapshot_index, snapshot_term, snapshot_data)) {
    snapshot_index_ = snapshot_index;
    snapshot_term_ = snapshot_term;

    LOG_INFO("Node {} snapshot saved at index {}, term {}", node_id_,
             snapshot_index_, snapshot_term_);

    // 通知日志管理器
    log_manager_->InstallSnapshot(snapshot_index_, snapshot_term_, {});
  }
}

} // namespace raft