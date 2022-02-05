# Author: Ming Zhang
# Copyright (c) 2021

set(WROKLOAD_SOURCES tatp.cc)

add_library(tatp STATIC ${WROKLOAD_SOURCES})

set_target_properties(tatp PROPERTIES LINKER_LANGUAGE CXX)


set(WORKLOAD_RUN main.cc tatp_bench.cc)

add_executable(tatp_bench ${WORKLOAD_RUN})

target_link_libraries(tatp_bench tatp stat ${WORKLOAD_LIB})