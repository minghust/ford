# Author: Ming Zhang
# Copyright (c) 2021

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
        include
        thirdparty
)

add_subdirectory(thirdparty/rlib)

add_subdirectory(src)

add_subdirectory(workload)

add_subdirectory(memory_pool)