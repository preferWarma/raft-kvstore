// HttpClient.h
#pragma once
#include <curl/curl.h>
#include <fstream>
#include <string>

namespace lyf {

class HttpClient {
public:
  struct UploadResult {
    bool success = false;
    long statusCode = 0;
    std::string response;
    std::string errorMessage;
  };

  inline HttpClient() {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
  }

  inline ~HttpClient() {
    if (curl) {
      curl_easy_cleanup(curl);
    }
    curl_global_cleanup();
  }

  // 上传文件到服务器
  inline UploadResult uploadFile(const std::string &url,
                                 const std::string &filePath,
                                 const std::string &fieldName = "file",
                                 const std::string &apiKey = "") {
    UploadResult result;

    if (!curl) {
      result.errorMessage = "CURL initialization failed";
      return result;
    }

    // 检查文件是否存在
    std::ifstream file(filePath, std::ios::binary);
    if (!file.is_open()) {
      result.errorMessage = "Cannot open file: " + filePath;
      return result;
    }
    file.close();

    curl_httppost *formpost = nullptr;
    curl_httppost *lastptr = nullptr;
    std::string responseString;

    // 添加文件到表单
    curl_formadd(&formpost, &lastptr, CURLFORM_COPYNAME, fieldName.c_str(),
                 CURLFORM_FILE, filePath.c_str(), CURLFORM_END);

    // 设置curl选项
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPPOST, formpost);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseString);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L); // 5分钟超时

    // 设置User-Agent
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "LogSystem/1.0");

    // 如果提供了API密钥，添加到请求头
    struct curl_slist *headers = nullptr;
    if (!apiKey.empty()) {
      std::string authHeader = "Authorization: Bearer " + apiKey;
      headers = curl_slist_append(headers, authHeader.c_str());
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    }

    // 执行请求
    CURLcode res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      result.errorMessage =
          "CURL error: " + std::string(curl_easy_strerror(res));
    } else {
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &result.statusCode);
      result.response = responseString;
      result.success = (result.statusCode >= 200 && result.statusCode < 300);
    }

    // 清理
    curl_formfree(formpost);
    if (headers) {
      curl_slist_free_all(headers);
    }

    return result;
  }

  // 发送GET请求
  inline UploadResult get(const std::string &url, const std::string &apiKey = "") {
    UploadResult result;

    if (!curl) {
      result.errorMessage = "CURL initialization failed";
      return result;
    }

    std::string responseString;

    // 设置curl选项
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseString);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L); // 30秒超时

    // 设置User-Agent
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "LogSystem/1.0");

    // 如果提供了API密钥，添加到请求头
    struct curl_slist *headers = nullptr;
    if (!apiKey.empty()) {
      std::string authHeader = "Authorization: Bearer " + apiKey;
      headers = curl_slist_append(headers, authHeader.c_str());
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    }

    // 执行请求
    CURLcode res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      result.errorMessage =
          "CURL error: " + std::string(curl_easy_strerror(res));
    } else {
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &result.statusCode);
      result.response = responseString;
      result.success = (result.statusCode >= 200 && result.statusCode < 300);
    }

    // 清理
    if (headers) {
      curl_slist_free_all(headers);
    }

    return result;
  }

private:
  CURL *curl;

  // curl写入回调函数
  inline static size_t WriteCallback(void *contents, size_t size, size_t nmemb,
                                     std::string *userp) {
    userp->append((char *)contents, size * nmemb);
    return size * nmemb;
  }
};

} // namespace lyf