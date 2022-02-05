# Author: Ming Zhang
# Copyright (c) 2021

set(WORKLOAD_INCLUDE
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/workload
        ${PROJECT_SOURCE_DIR}/thirdparty
        )

include_directories(${WORKLOAD_INCLUDE})

set(WORKLOAD_LIB ford)

add_subdirectory(stat)

add_subdirectory(tpcc)

add_subdirectory(tatp)

add_subdirectory(smallbank)

add_subdirectory(micro)