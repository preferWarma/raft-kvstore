#pragma once

#include <array>
#include <charconv>
#include <cstring>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

namespace lyf {

// ============================================================
// 高性能 Header-Only 格式化实现
// 特点：
// 1. 零依赖，纯 header-only
// 2. 最小化内存分配
// 3. 优化的类型转换
// 4. 内联友好
// 5. 支持编译期格式化
// ============================================================

namespace detail {

// ---------- 高性能类型转换工具 ----------
template <typename T>
inline size_t int_to_chars(char *buffer, size_t size, T value) {
  if constexpr (std::is_integral_v<T>) {
    auto [ptr, ec] = std::to_chars(buffer, buffer + size, value);
    return (ec == std::errc{}) ? (ptr - buffer) : 0;
  }
  return 0;
}

inline size_t float_to_chars(char *buffer, size_t size, double value) {
#if _LIBCPP_STD_VER >= 17
  auto [ptr, ec] = std::to_chars(buffer, buffer + size, value);
  return (ec == std::errc{}) ? (ptr - buffer) : 0;
#else
  int ret = std::snprintf(buffer, size, "%.6g", value);
  return (ret > 0 && ret < static_cast<int>(size)) ? ret : 0;
#endif
}

inline size_t bool_to_chars(char *buffer, size_t size, bool value) {
  constexpr const char *true_str = "true";
  constexpr const char *false_str = "false";
  const char *str = value ? true_str : false_str;
  size_t len = value ? 4 : 5;

  if (len <= size) {
    std::memcpy(buffer, str, len);
    return len;
  }
  return 0;
}

inline size_t ptr_to_chars(char *buffer, size_t size, const void *ptr) {
  if (size < 3)
    return 0;

  buffer[0] = '0';
  buffer[1] = 'x';

  uintptr_t value = reinterpret_cast<uintptr_t>(ptr);
  auto [end, ec] = std::to_chars(buffer + 2, buffer + size, value, 16);
  return (ec == std::errc{}) ? (end - buffer) : 0;
}

// ---------- 通用高性能 Appender ----------
template <typename T> struct FastAppender {
  static void append(std::string &out, T &&value) {
    using DecayT = std::decay_t<T>;

    if constexpr (std::is_same_v<DecayT, std::string> ||
                  std::is_same_v<DecayT, std::string_view>) {
      out.append(value);
    } else if constexpr (std::is_same_v<DecayT, const char *> ||
                         std::is_same_v<DecayT, char *>) {
      if (value)
        out.append(value);
      else
        out.append("(null)");
    } else if constexpr (std::is_same_v<DecayT, char>) {
      out.push_back(value);
    } else {
      // SSO思想：对于数值、指针、布尔等类型，使用栈上缓冲区转换
      char buffer[32]; // 短字符串采用栈上缓冲区
      size_t written = 0;

      if constexpr (std::is_same_v<DecayT, bool>) {
        written = bool_to_chars(buffer, sizeof(buffer), value);
      } else if constexpr (std::is_integral_v<DecayT>) {
        written = int_to_chars(buffer, sizeof(buffer), value);
      } else if constexpr (std::is_floating_point_v<DecayT>) {
        written =
            float_to_chars(buffer, sizeof(buffer), static_cast<double>(value));
      } else if constexpr (std::is_pointer_v<DecayT> ||
                           std::is_same_v<DecayT, std::nullptr_t>) {
        if (value)
          written = ptr_to_chars(buffer, sizeof(buffer), value);
        else {
          constexpr const char null_str[] = "nullptr";
          written = sizeof(null_str) - 1;
          std::memcpy(buffer, null_str, written);
        }
      }

      [[likely]] if (written > 0) {
        out.append(buffer, written);
      } else if constexpr (std::is_pointer_v<DecayT> ||
                           std::is_same_v<DecayT, std::nullptr_t>) {
        int ret = std::snprintf(buffer, sizeof(buffer), "%p", value);
        if (ret > 0 && ret < static_cast<int>(sizeof(buffer))) {
          out.append(buffer, ret);
        } else { // 兜底
          std::stringstream ss;
          ss << value;
          out.append(ss.str());
        }
      } else {
        // 对其他类型使用 to_string。
        out.append(std::to_string(std::forward<T>(value)));
      }
    }
  };
};

} // namespace detail

// ============================================================
// 运行时格式化实现
// ============================================================
namespace detail {

inline size_t find_placeholder(std::string_view str, size_t start = 0) {
  for (size_t i = start; i < str.size() - 1; ++i) {
    if (str[i] == '{' && str[i + 1] == '}') {
      return i;
    }
  }
  return std::string_view::npos;
}

template <typename... Args, size_t... Is>
inline void format_impl(std::string &result, std::string_view fmt,
                        std::index_sequence<Is...>, Args &&...args) {
  size_t pos = 0;
  auto process_arg = [&](auto &&arg) {
    size_t placeholder = find_placeholder(fmt, pos);
    if (placeholder != std::string_view::npos) {
      if (placeholder > pos) { // 处理占位符前的字面量
        result.append(fmt.data() + pos, placeholder - pos);
      }
      detail::FastAppender<decltype(arg)>::append(
          result, std::forward<decltype(arg)>(arg));
      pos = placeholder + 2;
    }
  };

  (process_arg(std::forward<Args>(args)), ...);

  if (pos < fmt.size()) { // 处理占位符后的字面量
    result.append(fmt.data() + pos, fmt.size() - pos);
  }
}
} // namespace detail

#if _LIBCPP_STD_VER >= 20
// ============================================================
// 编译期格式化实现
// ============================================================

// 编译期字符串容器
template <size_t N> struct FixedString {
  char data[N + 1] = {};
  size_t len = N;

  constexpr FixedString() = default;
  constexpr FixedString(const char (&str)[N + 1]) {
    for (size_t i = 0; i < N; ++i) {
      data[i] = str[i];
    }
    data[N] = '\0';
    len = N;
  }

  constexpr std::string_view view() const {
    return std::string_view(data, len);
  }

  constexpr const char *c_str() const { return data; }
  constexpr size_t size() const { return len; }
};

// 编译期字符串字面量推导
template <size_t N> FixedString(const char (&)[N]) -> FixedString<N - 1>;

// 编译期格式解析器
template <size_t N> struct FormatInfo {
  static constexpr size_t MAX_SEGMENTS = 64;
  // 段类型：字面量或占位符
  enum SegmentType : uint8_t { Literal, Placeholder };

  struct Segment {
    SegmentType type;
    size_t start;  // 对于字面量：起始位置
    size_t length; // 对于字面量：长度
  };

  FixedString<N> format;
  std::array<Segment, MAX_SEGMENTS> segments = {};
  size_t segment_count = 0;
  size_t placeholder_count = 0;

  constexpr FormatInfo(const char (&fmt)[N + 1]) : format(fmt) { parse(); }

private:
  constexpr void parse() {
    size_t pos = 0;
    while (pos < N) {
      // 查找下一个占位符
      size_t placeholder_pos = find_next_placeholder(pos);
      if (placeholder_pos == N) {
        // 没有更多占位符，记录剩余的字面量
        if (pos < N) {
          segments[segment_count++] = {Literal, pos, N - pos};
        }
        break;
      }
      // 记录占位符前的字面量(如果有)
      if (placeholder_pos > pos) {
        segments[segment_count++] = {Literal, pos, placeholder_pos - pos};
      }
      // 记录占位符(占位符不需要位置和长度信息)
      segments[segment_count++] = {Placeholder, 0, 0};
      placeholder_count++;
      pos = placeholder_pos + 2; // 跳过 '{}'
    }
  }

  constexpr size_t find_next_placeholder(size_t start) const {
    for (size_t i = start; i < N - 1; ++i) {
      if (format.data[i] == '{' && format.data[i + 1] == '}') {
        return i;
      }
    }
    return N;
  }
};

// 编译期格式化核心实现
namespace detail {

template <auto Info, typename... Args>
std::string format_compile_impl(Args &&...args) {
  static_assert(Info.placeholder_count == sizeof...(args),
                "Number of arguments doesn't match format string placeholders");

  std::string result;
  // 预估大小：格式字符串长度 + 每个参数平均10字符
  size_t estimated_size = Info.format.size() + sizeof...(args) * 10;
  result.reserve(estimated_size);

  if constexpr (sizeof...(args) == 0) {
    // 无参数时，只输出字面量
    for (size_t i = 0; i < Info.segment_count; ++i) {
      const auto &segment = Info.segments[i];
      if (segment.type == decltype(Info)::Literal) {
        result.append(Info.format.data + segment.start, segment.length);
      }
    }
    return result;
  }

  auto args_tuple = std::forward_as_tuple(std::forward<Args>(args)...);
  size_t arg_index = 0;

  // 处理参数输出
  auto append_arg_at_index = [&result, &args_tuple](size_t target_index) {
    size_t current_index = 0;
    // 遍历参数元组，找到目标索引的参数
    std::apply(
        [&](auto &&...elements) {
          ((current_index++ == target_index
                ? (detail::FastAppender<decltype(elements)>::append(
                       result, std::forward<decltype(elements)>(elements)),
                   0)
                : 0),
           ...);
        },
        args_tuple);
  };

  // 线性遍历段
  for (size_t i = 0; i < Info.segment_count; ++i) {
    const auto &segment = Info.segments[i];

    if (segment.type == decltype(Info)::Literal) {
      // 直接使用预计算的位置和长度
      result.append(Info.format.data + segment.start, segment.length);
    } else {
      // 输出对应索引的参数
      if (arg_index < sizeof...(args)) {
        append_arg_at_index(arg_index);
        arg_index++;
      }
    }
  }

  return result;
}
} // namespace detail

// ============================================================
// 编译期格式化接口
// ============================================================

template <FixedString fmt> struct CompileFormat {
  static constexpr auto info = FormatInfo<fmt.len>(fmt.data);

  template <typename... Args> std::string operator()(Args &&...args) const {
    return detail::format_compile_impl<info>(std::forward<Args>(args)...);
  }
};

// 用户定义字面量
template <FixedString fmt> constexpr auto operator""_fmt() {
  return CompileFormat<fmt>{};
}

// 宏接口
#define FMT(str)                                                               \
  lyf::CompileFormat<lyf::FixedString(str)> {}

#endif

// ============================================================
// 统一接口
// ============================================================

// 运行时格式化
template <typename... Args>
[[nodiscard]] inline std::string FormatMessage(std::string_view fmt,
                                               Args &&...args) {
  if constexpr (sizeof...(args) == 0) {
    return std::string(fmt);
  }
  size_t estimated_size = fmt.size() + sizeof...(args) * 8;
  std::string result;
  result.reserve(estimated_size);
  detail::format_impl(result, fmt, std::index_sequence_for<Args...>{},
                      std::forward<Args>(args)...);
  return result;
}

#if _LIBCPP_STD_VER >= 20
// 编译期格式化
template <FixedString fmt, typename... Args>
[[nodiscard]] inline std::string FormatMessage(Args &&...args) {
  if constexpr (sizeof...(args) == 0) {
    return std::string(fmt.c_str(), fmt.size());
  }
  return CompileFormat<fmt>{}(std::forward<Args>(args)...);
}
#endif

// ============================================================
// 使用示例
// ============================================================

/*
// 运行时格式化
auto rt = FormatMessage("Value: {}", 42);

// 编译期格式化（方法1：使用字面量）
constexpr auto fmt = "Value: {}"_fmt;
auto ct1 = fmt(42);

// 编译期格式化（方法2：使用宏）
auto ct2 = FMT("Value: {}")(42);

// 编译期格式化（方法3：使用模板参数）
auto ct3 = FormatMessage<"Value: {}">(42);
*/

} // namespace lyf