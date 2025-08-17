#include "storage/bloom_filter.h"
#include <functional>

namespace storage {

BloomFilter::BloomFilter(size_t bit_size, size_t hash_count)
  : bits_(bit_size), hash_count_(hash_count) {}

void BloomFilter::Add(const std::string& key) {
  for (size_t i = 0; i < hash_count_; i++) {
    size_t index = Hash(key, i) % bits_.size();
    bits_[index] = true;
  }
}

bool BloomFilter::MayExist(const std::string& key) const {
  for (size_t i = 0; i < hash_count_; i++) {
    size_t index = Hash(key, i) % bits_.size();
    if (!bits_[index]) {
      return false;
    }
  }
  return true;
}

void BloomFilter::Reset() {
  std::fill(bits_.begin(), bits_.end(), false);
}

size_t BloomFilter::Hash(const std::string& key, size_t seed) const {
  // 使用std::hash作为基础哈希函数
  std::hash<std::string> hasher;
  size_t hash = hasher(key);
  
  // 使用种子值来生成不同的哈希函数
  // 这是一个简单的线性组合方法
  return hash + seed * 2654435761U; // 2654435761U 是一个大质数
}

} // namespace storage