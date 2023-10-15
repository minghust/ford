#include "batch_txn.h"

std::string BatchTxn::get_log_string() {
    std::string log_string = "";
    for(auto log: logs) {
        char* temp_log_char = new char[log->log_tot_len_];
        log->serialize(temp_log_char);
        std::string temp_log_string = temp_log_char;
        log_string += temp_log_string;
        delete[] temp_log_char;
    }
    return log_string;
}