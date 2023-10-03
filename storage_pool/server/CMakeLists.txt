# Author: Ming Zhang
# Copyright (c) 2022

set(SERVER_SOURCE server.cc)
add_executable(storage_pool ${SERVER_SOURCE})
target_link_libraries(storage_pool ford tatp_db smallbank_db tpcc_db micro_db rlib ${DYNAMIC_LIB} ${BRPC_LIB})
