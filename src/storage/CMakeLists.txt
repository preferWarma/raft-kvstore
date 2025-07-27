# src/storage/CMakeLists.txt
file(GLOB_RECURSE STORAGE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_library(storage_lib STATIC ${STORAGE_SOURCES})
include_directories(${CMAKE_SOURCE_DIR}/include)

target_link_libraries(storage_lib
    proto_lib
    ${ROCKSDB_LIB}
    Threads::Threads
)

target_include_directories(storage_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${PROTO_INCLUDE_DIR}
)