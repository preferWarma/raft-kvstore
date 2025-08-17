#pragma once

#include <string>
#include <vector>

namespace storage {

class BloomFilter {
public:
  // 构造函数，指定位数组大小和哈希函数数量
  BloomFilter(size_t bit_size, size_t hash_count);
  
  // 默认析构函数
  ~BloomFilter() = default;
  
  // 添加一个键到布隆过滤器
  void Add(const std::string& key);
  
  // 检查键是否可能存在于布隆过滤器中
  bool MayExist(const std::string& key) const;
  
  // 重置布隆过滤器
  void Reset();
  
private:
  // 位数组
  std::vector<bool> bits_;
  
  // 哈希函数数量
  size_t hash_count_;
  
  // 哈希函数实现
  size_t Hash(const std::string& key, size_t seed) const;
};

} // namespace storage