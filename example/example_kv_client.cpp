// example/example_kv_client.cpp
#include "rpc/kv_client.h"
#include <iostream>

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <server_address>" << std::endl;
    return 1;
  }

  kvstore::KVClient client(argv[1]);

  std::string cmd, key, value;
  while (std::cin >> cmd) {
    if (cmd == "put") {
      std::cin >> key >> value;
      if (client.Put(key, value)) {
        std::cout << "OK" << std::endl;
      } else {
        std::cout << "Error: " << client.GetLastError() << std::endl;
      }
    } else if (cmd == "get") {
      std::cin >> key;
      if (client.Get(key, &value)) {
        std::cout << value << std::endl;
      } else {
        std::cout << "Error: " << client.GetLastError() << std::endl;
      }
    } else if (cmd == "delete") {
      std::cin >> key;
      if (client.Delete(key)) {
        std::cout << "OK" << std::endl;
      } else {
        std::cout << "Error: " << client.GetLastError() << std::endl;
      }
    } else if (cmd == "quit") {
      break;
    }
  }

  return 0;
}