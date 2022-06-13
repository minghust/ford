# Author: Ming Zhang
# Copyright (c) 2022

set(SERVER_SOURCE server.cc)
add_executable(zm_mem_pool ${SERVER_SOURCE})
target_link_libraries(zm_mem_pool tatp_db smallbank_db tpcc_db micro_db rlib)
