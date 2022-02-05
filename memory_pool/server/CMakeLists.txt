# Author: Ming Zhang
# Copyright (c) 2021

set(SERVER_SOURCE server.cc)
add_executable(server ${SERVER_SOURCE})
target_link_libraries(server tatp smallbank tpcc micro rlib)
