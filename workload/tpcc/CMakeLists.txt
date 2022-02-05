# Author: Ming Zhang
# Copyright (c) 2021

set(WROKLOAD_SOURCES tpcc.cc)

add_library(tpcc STATIC ${WROKLOAD_SOURCES})

set_target_properties(tpcc PROPERTIES LINKER_LANGUAGE CXX)


set(WORKLOAD_RUN main.cc tpcc_bench.cc)

add_executable(tpcc_bench ${WORKLOAD_RUN})

target_link_libraries(tpcc_bench tpcc stat ${WORKLOAD_LIB})