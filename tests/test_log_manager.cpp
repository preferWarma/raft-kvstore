// tests/test_log_manager.cpp

#include "raft/log_manager.h"
#include "raft/raft_state.h"
#include <gtest/gtest.h>

using namespace raft;

class LogManagerTest : public ::testing::Test {
protected:
  void SetUp() override { log_manager_ = std::make_unique<LogManager>(); }

  LogEntry CreateEntry(uint64_t index, uint64_t term,
                       const std::string &cmd = "") {
    LogEntry entry;
    entry.set_index(index);
    entry.set_term(term);
    entry.set_type(LogEntry::NORMAL);
    entry.set_command(cmd);
    return entry;
  }

  std::unique_ptr<LogManager> log_manager_;
};

TEST_F(LogManagerTest, InitialState) {
  EXPECT_EQ(log_manager_->GetLastIndex(), 0);
  EXPECT_EQ(log_manager_->GetLastTerm(), 0);
  EXPECT_EQ(log_manager_->GetLogSize(), 1); // 包含哨兵条目
}

TEST_F(LogManagerTest, AppendSingleEntry) {
  auto entry = CreateEntry(1, 1, "command1");
  log_manager_->Append(entry);

  EXPECT_EQ(log_manager_->GetLastIndex(), 1);
  EXPECT_EQ(log_manager_->GetLastTerm(), 1);

  auto retrieved = log_manager_->GetEntry(1);
  EXPECT_EQ(retrieved.index(), 1);
  EXPECT_EQ(retrieved.term(), 1);
  EXPECT_EQ(retrieved.command(), "command1");
}

TEST_F(LogManagerTest, AppendMultipleEntries) {
  for (uint64_t i = 1; i <= 5; ++i) {
    log_manager_->Append(CreateEntry(i, 1, "cmd" + std::to_string(i)));
  }

  EXPECT_EQ(log_manager_->GetLastIndex(), 5);
  EXPECT_EQ(log_manager_->GetLastTerm(), 1);

  auto entries = log_manager_->GetEntries(2, 5);
  EXPECT_EQ(entries.size(), 3);
  EXPECT_EQ(entries[0].index(), 2);
  EXPECT_EQ(entries[2].index(), 4);
}

TEST_F(LogManagerTest, TruncateLog) {
  for (uint64_t i = 1; i <= 10; ++i) {
    log_manager_->Append(CreateEntry(i, i, "cmd"));
  }

  log_manager_->TruncateAfter(5);

  EXPECT_EQ(log_manager_->GetLastIndex(), 5);
  EXPECT_EQ(log_manager_->GetLastTerm(), 5);
}

TEST_F(LogManagerTest, LogMatching) {
  log_manager_->Append(CreateEntry(1, 1));
  log_manager_->Append(CreateEntry(2, 1));
  log_manager_->Append(CreateEntry(3, 2));

  EXPECT_TRUE(log_manager_->MatchLog(2, 1));
  EXPECT_TRUE(log_manager_->MatchLog(3, 2));
  EXPECT_FALSE(log_manager_->MatchLog(2, 2));
  EXPECT_FALSE(log_manager_->MatchLog(4, 1));
}

TEST_F(LogManagerTest, ConflictDetection) {
  log_manager_->Append(CreateEntry(1, 1));
  log_manager_->Append(CreateEntry(2, 1));
  log_manager_->Append(CreateEntry(3, 2));
  log_manager_->Append(CreateEntry(4, 2));

  auto [conflict_idx, conflict_term] = log_manager_->FindConflict(3, 1);
  EXPECT_EQ(conflict_idx, 3);
  EXPECT_EQ(conflict_term, 2);
}

TEST_F(LogManagerTest, AppendEntriesWithConflict) {
  // 初始日志
  log_manager_->Append(CreateEntry(1, 1));
  log_manager_->Append(CreateEntry(2, 1));
  log_manager_->Append(CreateEntry(3, 2));

  // 新条目与现有条目冲突
  std::vector<LogEntry> new_entries = {CreateEntry(3, 3, "new3"),
                                       CreateEntry(4, 3, "new4")};

  log_manager_->AppendEntries(2, 1, new_entries);

  EXPECT_EQ(log_manager_->GetLastIndex(), 4);
  EXPECT_EQ(log_manager_->GetLastTerm(), 3);
  EXPECT_EQ(log_manager_->GetEntry(3).term(), 3);
  EXPECT_EQ(log_manager_->GetEntry(3).command(), "new3");
}

TEST_F(LogManagerTest, Snapshot) {
  // 添加一些日志
  for (uint64_t i = 1; i <= 10; ++i) {
    log_manager_->Append(CreateEntry(i, 1));
  }

  // 安装快照
  std::vector<LogEntry> remaining = {CreateEntry(8, 1), CreateEntry(9, 1),
                                     CreateEntry(10, 1)};

  log_manager_->InstallSnapshot(7, 1, remaining);

  EXPECT_EQ(log_manager_->GetSnapshotIndex(), 7);
  EXPECT_EQ(log_manager_->GetSnapshotTerm(), 1);
  EXPECT_EQ(log_manager_->GetLastIndex(), 10);

  // 验证无法获取快照中的日志
  auto entry = log_manager_->GetEntry(5);
  EXPECT_EQ(entry.term(), 1); // 只能获取到任期
}

// 测试Raft状态转换
TEST(RaftNodeTest, StateTransitions) {
  std::vector<std::string> peers = {"node2", "node3"};
  RaftNode node("node1", peers, "./test_data");

  EXPECT_EQ(node.GetState(), NodeState::FOLLOWER);
  EXPECT_EQ(node.GetCurrentTerm(), 0);

  // 测试状态转换（需要友元类或测试接口）
  // 这里只是示例，实际需要适当的测试接口
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}