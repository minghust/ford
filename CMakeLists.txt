# Author: Ming Zhang
# Copyright (c) 2022

cmake_minimum_required(VERSION 3.3)

project(FORD)

# option(WITH_SNAPPY "With snappy" ON)

execute_process(
    COMMAND bash -c "find ${PROJECT_SOURCE_DIR}/ -type d -regex \".*output/include$\" | head -n1 | xargs dirname | tr -d '\n'"
    OUTPUT_VARIABLE OUTPUT_PATH
)
set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result -fPIC")

include(FindThreads)
include(FindProtobuf)

find_library(THRIFT_LIB NAMES thrift)
if (NOT THRIFT_LIB)
    set(THRIFT_LIB "")
endif()
find_library(THRIFTNB_LIB NAMES thriftnb)
if (NOT THRIFTNB_LIB)
    set(THRIFTNB_LIB "")
endif()

find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
if(LINK_SO)
    find_library(BRPC_LIB NAMES brpc)
else()
    find_library(BRPC_LIB NAMES libbrpc.a brpc)
endif()
if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()
include_directories(${BRPC_INCLUDE_PATH})

find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
include_directories(${GFLAGS_INCLUDE_PATH})

find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()
include_directories(${LEVELDB_INCLUDE_PATH})

# if(WITH_SNAPPY)
find_path(SNAPPY_INCLUDE_PATH NAMES snappy.h)
find_library(SNAPPY_LIB NAMES snappy)
if ((NOT SNAPPY_INCLUDE_PATH) OR (NOT SNAPPY_LIB))
    message(FATAL_ERROR "Fail to find snappy")
endif()
include_directories(${SNAPPY_INCLUDE_PATH})
# endif()

find_package(OpenSSL)
include_directories(${OPENSSL_INCLUDE_DIR})

find_path(RDMA_INCLUDE_PATH NAMES infiniband/verbs.h)
find_library(RDMA_LIB NAMES ibverbs)
if ((NOT RDMA_INCLUDE_PATH) OR (NOT RDMA_LIB))
    message(FATAL_ERROR "Fail to find ibverbs")
endif()

set(DYNAMIC_LIB
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${PROTOBUF_LIBRARIES}
    ${LEVELDB_LIB}
    ${SNAPPY_LIB}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${OPENSSL_SSL_LIBRARY}
    ${THRIFT_LIB}
    ${THRIFTNB_LIB}
    dl
    )
    
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -DNDEBUG")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -DDEBUG -g")
endif()

include_directories(
    core
    workload
    thirdparty
    compute_pool
)

add_subdirectory(thirdparty/rlib)

add_subdirectory(core)

add_subdirectory(workload)

add_subdirectory(test) 

add_subdirectory(compute_pool) # Dep list: rlib->ford->workload_db+_txn->worker

add_subdirectory(memory_pool) # Dep list: rlib->workload_db->server

add_subdirectory(storage_pool) 