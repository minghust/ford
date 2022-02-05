# Author: Ming Zhang
# Copyright (c) 2021

set(WROKLOAD_SOURCES micro.cc)

add_library(micro STATIC ${WROKLOAD_SOURCES})

set_target_properties(micro PROPERTIES LINKER_LANGUAGE CXX)


set(WORKLOAD_RUN main.cc micro_bench.cc)

add_executable(micro_bench ${WORKLOAD_RUN})

target_link_libraries(micro_bench micro stat ${WORKLOAD_LIB})