#pragma once
#include <iostream>
#include <memory>
#include <mutex>

namespace lyf {

template <typename T> class Singleton { // 泛型单例
public:
  // 获取单例实例对象
  static T &GetInstance() {
    // 利用局部静态变量实现单例
    std::call_once(once_flag_, []() {
      std::unique_ptr<T> t(new T());
      instance_.reset(t.release());
    });
    return *instance_;
  }

  // 打印单例的地址
  void printAddress() { std::cout << this << std::endl; }

  // 禁止外部拷贝或赋值
  Singleton(const Singleton &) = delete;
  Singleton(Singleton &&) = delete;
  Singleton &operator=(const Singleton &) = delete;
  Singleton &operator=(Singleton &&) = delete;

protected:
  // 禁止外部构造和析构
  Singleton() = default;
  ~Singleton() = default;
private:
  static std::once_flag once_flag_;
  static std::unique_ptr<T> instance_;
};


template <typename T>
std::unique_ptr<T> Singleton<T>::instance_ = nullptr;

template <typename T>
std::once_flag Singleton<T>::once_flag_{};


} // namespace lyf
