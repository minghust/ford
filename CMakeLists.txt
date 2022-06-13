# Author: Ming Zhang
# Copyright (c) 2022

cmake_minimum_required(VERSION 3.3)

project(FORD)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result -fPIC")

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

add_subdirectory(compute_pool) # Dep list: rlib->ford->workload_db+_txn->worker

add_subdirectory(memory_pool) # Dep list: rlib->workload_db->server