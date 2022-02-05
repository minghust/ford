# Author: Ming Zhang
# Copyright (c) 2021

set(MEMORY_NODE_INCLUDE
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/workload
        ${PROJECT_SOURCE_DIR}/thirdparty
        )

include_directories(${MEMORY_NODE_INCLUDE})

add_subdirectory(server)
