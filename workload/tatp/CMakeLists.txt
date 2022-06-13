# Author: Ming Zhang
# Copyright (c) 2022

set(DB_SOURCES tatp_db.cc)
set(TXN_SOURCES tatp_txn.cc)

add_library(tatp_db STATIC ${DB_SOURCES})
add_library(tatp_txn STATIC ${TXN_SOURCES})

set_target_properties(tatp_db PROPERTIES LINKER_LANGUAGE CXX)
set_target_properties(tatp_txn PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(tatp_txn ford)