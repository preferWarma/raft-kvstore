# example/CMakeLists.txt
add_executable(example_cluster example_cluster.cpp)
target_link_libraries(example_cluster PRIVATE
    raft_lib
    storage_lib
    rpc_lib
    proto_lib
)

add_executable(example_kv_client example_kv_client.cpp)
target_link_libraries(example_kv_client PRIVATE
    rpc_lib
    proto_lib
)