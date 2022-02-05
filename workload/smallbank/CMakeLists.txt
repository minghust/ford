# Author: Ming Zhang
# Copyright (c) 2021

set(WROKLOAD_SOURCES smallbank.cc)

add_library(smallbank STATIC ${WROKLOAD_SOURCES})

set_target_properties(smallbank PROPERTIES LINKER_LANGUAGE CXX)


set(WORKLOAD_RUN main.cc smallbank_bench.cc)

add_executable(smallbank_bench ${WORKLOAD_RUN})

target_link_libraries(smallbank_bench smallbank stat ${WORKLOAD_LIB})