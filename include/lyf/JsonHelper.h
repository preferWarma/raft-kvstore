#include "Singleton.h"
#include "Helper.h"
#include "third/json.hpp" // nlohmann json库

#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace lyf {

using std::string, std::vector, std::shared_ptr;
using json = nlohmann::json;
namespace fs = std::filesystem;

class JsonHelper : public Singleton<JsonHelper> {
  friend class Singleton<JsonHelper>;

private:
  json config_data;
  string config_file_path;

public:
  JsonHelper(const string &file_path = "config.json")
      : config_file_path(file_path) {
    if (fs::exists(config_file_path)) {
      if (!LoadFromFile()) {
        throw std::runtime_error("配置文件加载失败: " + config_file_path);
      }
    } else {
      // 如果文件不存在，创建一个空的json对象
      lyf_Internal_LOG(">>>>>>配置文件不存在，将创建一个空的配置文件: {}\n",
                      config_file_path.c_str());
      config_data = json::object();
    }
  }

public: // 加载和保存操作
  // 从文件加载配置
  bool LoadFromFile() {
    std::ifstream file(config_file_path);
    if (!file.is_open()) {
      lyf_Internal_LOG("无法打开配置文件: {}\n", config_file_path.c_str());
      return false;
    }

    try {
      config_data = json::parse(file);
      size_t top_level_keys = config_data.size();
      size_t total_properties = CountLeafProperties(config_data);
      lyf_Internal_LOG(
          ">>>>>>配置文件({})加载成功: 共{}个配置, {}个配置属性\n",
          fs::absolute(config_file_path).c_str(), top_level_keys,
          total_properties);
      return true;
    } catch (const json::parse_error &e) {
      lyf_Internal_LOG("JSON解析失败: {}\n", e.what());
      return false;
    } catch (const std::exception &e) {
      lyf_Internal_LOG("配置加载失败: {}\n", e.what());
      return false;
    }
  }

  // 保存配置到文件
  bool SaveToFile() {
    std::ofstream file(config_file_path);
    if (!file.is_open()) {
      lyf_Internal_LOG("无法创建配置文件: {}\n", config_file_path.c_str());
      return false;
    }

    file << config_data.dump(4) << std::endl;
    file.close();
    lyf_Internal_LOG("配置文件保存成功: {}, 共{}个配置, {}个配置属性\n",
                    fs::absolute(config_file_path).c_str(), config_data.size(),
                    CountLeafProperties(config_data));
    return true;
  }

  // 设置配置文件路径
  void SetFilePath(const string &path) { config_file_path = path; }

  // 获取配置文件路径
  const string &getFilePath() const { return config_file_path; }

  // 重新加载配置文件
  bool ReloadFromFile(const string other_path = "") {
    if (fs::exists(other_path) &&
        fs::absolute(other_path) != fs::absolute(config_file_path)) {
      SetFilePath(other_path);
      lyf_Internal_LOG(">>>>>>配置文件已更新为: {}\n",
                      fs::absolute(config_file_path).c_str());
    }
    return LoadFromFile();
  }

public: // 配置项操作
  // 设置配置项（支持嵌套，使用点号分隔）
  template <typename T> void Set(const string &key, const T &value) {
    config_data[json::json_pointer(KeyToPtr(key))] = value;
  }

  // 获取配置项（带默认值）
  template <typename T> T Get(const string &key, const T &default_value = T{}) {
    try {
      return config_data.at(json::json_pointer(KeyToPtr(key))).get<T>();
    } catch (const json::out_of_range &) { // 键不存在,返回默认值
      return default_value;
    } catch (const json::type_error &) {
      lyf_Internal_LOG("配置项 '{}' 类型不匹配，期望类型: {}\n", key.c_str(),
                      typeid(T).name());
      return default_value;
    }
  }

  // 便利方法：设置组配置
  template <typename T>
  void SetWithGroup(const string &group, const string &key, const T &value) {
    Set(group + "." + key, value);
  }

  // 便利方法：获取组配置
  template <typename T>
  T GetWithGroup(const string &group, const string &key,
                 const T &default_value = T{}) {
    return Get(group + "." + key, default_value);
  }

  // 检查配置项是否存在
  bool Has(const string &key) const {
    return !config_data.value(json::json_pointer(KeyToPtr(key)), json())
                .is_null();
  }

  // 检查组是否存在
  bool HasGroup(const string &group) const { return Has(group); }

  // 删除配置项
  bool Remove(const string &key) {
    try {
      size_t last_dot = key.find_last_of('.');
      if (last_dot == string::npos) {
        // Top-level key
        return config_data.erase(key) > 0;
      }

      string parent_key = key.substr(0, last_dot);
      string child_key = key.substr(last_dot + 1);

      json::json_pointer parent_ptr(KeyToPtr(parent_key));
      json &parent_obj = config_data.at(parent_ptr);

      if (parent_obj.is_object()) {
        return parent_obj.erase(child_key) > 0;
      }
      return false;

    } catch (const json::out_of_range &) {
      return false; // Parent key doesn't exist
    }
  }

  // 删除整个组
  bool RemoveGroup(const string &group) { return Remove(group); }

  // 获取组下的所有键
  vector<string> GetGroupKeys(const string &group) const {
    vector<string> keys;
    try {
      auto group_obj = config_data.at(json::json_pointer(KeyToPtr(group)));
      if (group_obj.is_object()) {
        for (auto &el : group_obj.items()) {
          keys.push_back(el.key());
        }
      }
    } catch (const json::out_of_range &) {
      // group not found
    }
    return keys;
  }

  // 获取所有组名
  vector<string> GetAllGroups() const {
    vector<string> groups;
    if (config_data.is_object()) {
      for (auto &el : config_data.items()) {
        if (el.value().is_object()) {
          groups.push_back(el.key());
        }
      }
    }
    return groups;
  }

  // 清空所有配置
  void Clear() { config_data.clear(); }

  // 获取所有配置项的键
  vector<string> GetAllKeys() const {
    vector<string> keys;
    if (config_data.is_object()) {
      for (auto &el : config_data.items()) {
        keys.push_back(el.key());
      }
    }
    return keys;
  }

public: // 打印操作
  // 打印所有配置项
  void PrintAllConfig() const {
    std::cout << "=== 当前配置 ===" << std::endl;
    std::cout << config_data.dump(4) << std::endl;
    std::cout << "=================" << std::endl;
  }

  // 打印指定组的配置
  void PrintGroup(const string &group) const {
    std::cout << "=== " << group << " 配置 ===" << std::endl;
    try {
      auto group_obj = config_data.at(json::json_pointer(KeyToPtr(group)));
      std::cout << group_obj.dump(4) << std::endl;
    } catch (const json::out_of_range &) {
      std::cout << "(该组不存在或为空)" << std::endl;
    }
    std::cout << "==================" << std::endl;
  }

  // 获取解析统计信息
  void printStatistics() const {
    std::cout << "=== 配置统计 ===\n"
              << "总配置项: " << Size() << "\n"
              << "总配置属性: " << PropertySize() << "\n"
              << "=================" << std::endl;
  }

public: // 统计操作
  // 获取配置项数量
  size_t Size() const { return config_data.size(); }

  // 获取配置属性数量
  size_t PropertySize() const { return CountLeafProperties(config_data); }

private:
  size_t CountLeafProperties(const json &j) const {
    size_t count = 0;
    if (j.is_object()) {
      for (auto &el : j.items()) {
        if (el.value().is_primitive()) {
          count++;
        } else {
          count += CountLeafProperties(el.value());
        }
      }
    }
    return count;
  }

  // 辅助函数：将点号分隔的键转换为JSON指针
  // example: "a.b.c" -> "/a/b/c"
  string KeyToPtr(const string &key) const {
    string ptr = "/";
    for (char c : key) {
      if (c == '.') {
        ptr += '/';
      } else {
        ptr += c;
      }
    }
    return ptr;
  }
};

} // namespace lyf