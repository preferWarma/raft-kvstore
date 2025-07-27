# raft-kvdb

基于Raft共识算法的分布式键值数据库实现

## 项目概述

一个使用C++实现的分布式键值存储系统，基于Raft共识算法确保数据一致性。RPC 模块采用 gRPC 实现，存储层采用 RocksDB 实现。

## 主要特性

- 基于Raft共识算法实现分布式一致性
- 支持基本的键值对操作(GET/PUT/DELETE)
- 使用RocksDB作为底层存储引擎
- 模块化设计，包含Raft核心、RPC通信和存储层
- 完整的测试套件确保系统可靠性

## 项目结构

```
raft-kvdb/
├── .gitignore
├── CMakeLists.txt
├── example/           # 示例程序
├── include/           # 头文件
│   ├── lyf/           # 通用工具类
│   ├── raft/          # Raft算法相关
│   ├── rpc/           # RPC通信相关
│   └── storage/       # 存储相关
├── src/               # 源代码
│   ├── proto/         # Protobuf定义
│   ├── raft/          # Raft算法实现
│   ├── rpc/           # RPC通信实现
│   └── storage/       # 存储实现
└── tests/             # 测试代码
```

## 构建与安装

### 环境要求
- C++17或更高版本
- CMake 3.10+ 
- Protobuf 3.0+
- RocksDB
- gRPC

### 编译步骤

```bash
# 克隆仓库
git clone https://github.com/preferWarma/raft-kvstore.git
cd raft-kvstore

# 创建构建目录
mkdir build && cd build

# 生成Makefile
cmake ..

# 编译
make -j4

# 运行测试
cd build/test
ctest
```

## 使用示例

### 启动集群

```bash
# 启动三个节点的集群示例
./example/example_cluster 3
```

### 客户端操作

```cpp
// 示例代码来自example/example_kv_client.cpp
#include <rpc/kv_client.h>

int main() {
    // 连接到Raft集群
    KVClient client({"127.0.0.1:50051", "127.0.0.1:50052", "127.0.0.1:50053"});

    // 插入键值对
    client.put("key1", "value1");

    // 获取值
    std::string value = client.get("key1");
    std::cout << "key1: " << value << std::endl;

    // 删除键
    client.del("key1");

    return 0;
}
```

## 测试

项目包含完整的测试套件，涵盖Raft算法的各个方面：

- Leader选举测试
- 日志复制测试
- 键值存储集成测试
- RPC通信测试
- 日志管理器测试

运行所有测试：
```bash
cd build
make test
```

## 许可证

[MIT许可证](LICENSE)