# Author: Ming Zhang
# Copyright (c) 2021

set(WROKLOAD_SOURCES result_collect.cc)
add_library(stat STATIC ${WROKLOAD_SOURCES})
set_target_properties(stat PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(stat ${WORKLOAD_LIB})