#include <assert.h>

#include "logreplay.h"

void LogReplay::apply_sigle_log(LogRecord* log) {
    switch(log->log_type_) {
        case LogType::INSERT: {
            InsertLogRecord* insert_log = dynamic_cast<InsertLogRecord*>(log);
            std::string table_name(insert_log->table_name_, insert_log->table_name_ + insert_log->table_name_size_);
            int fd = disk_manager_->get_file_fd(table_name);
            // 每个表的元数据应该就放在表的第一页，每个页面的元数据放在页面的头部，所以对元数据的更改直接修改对应页面就行了
            // disk_manager_->set_bitmap(fd, insert_log->rid_.page_no_, insert_log->slot_no_);
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FIRST_FREE_PAGE_NO, (char*)(&insert_log->first_free_page_no_), sizeof(int));
            disk_manager_->update_value(fd, insert_log->rid_.page_no_, OFFSET_NUM_RECORDS, (char*)(&insert_log->num_records_), sizeof(int));
            disk_manager_->update_value(fd, insert_log->rid_.page_no_, insert_log->bucket_offset_, &insert_log->bucket_value_, sizeof(char));
            disk_manager_->update_value(fd, insert_log->rid_.page_no_, insert_log->rid_.slot_offset_, (char*)&insert_log->insert_value_.key_, sizeof(itemkey_t));
            disk_manager_->update_value(fd, insert_log->rid_.page_no_, insert_log->rid_.slot_offset_ + sizeof(itemkey_t), insert_log->insert_value_.value_, insert_log->insert_value_.value_size_ * sizeof(char));
        } break;
        case LogType::DELETE: {
            DeleteLogRecord* delete_log = dynamic_cast<DeleteLogRecord*>(log);
            std::string table_name(delete_log->table_name_, delete_log->table_name_ + delete_log->table_name_size_);
            int fd = disk_manager_->get_file_fd(table_name);
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FIRST_FREE_PAGE_NO, (char*)(&delete_log->first_free_page_no_), sizeof(int));
            disk_manager_->update_value(fd, delete_log->page_no_, OFFSET_PAGE_HDR, (char*)&delete_log->page_hdr_, sizeof(RmPageHdr));
            disk_manager_->update_value(fd, delete_log->page_no_, delete_log->bucket_offset_, &delete_log->bucket_value_, sizeof(char));
        } break;
        case LogType::UPDATE: {
            UpdateLogRecord* update_log = dynamic_cast<UpdateLogRecord*>(log);
            std::string table_name(update_log->table_name_, update_log->table_name_ + update_log->table_name_size_);
            int fd = disk_manager_->get_file_fd(table_name);
            disk_manager_->update_value(fd, update_log->rid_.page_no_, update_log->rid_.slot_offset_ + sizeof(itemkey_t), update_log->new_value_.value_, update_log->new_value_.value_size_ * sizeof(char));
        }
        case LogType::NEWPAGE: {
            NewPageLogRecord* new_page_log = dynamic_cast<NewPageLogRecord*>(log);
            std::string table_name(new_page_log->table_name_, new_page_log->table_name_ + new_page_log->table_name_size_);
            int fd = disk_manager_->get_file_fd(table_name);
            int page_no = disk_manager_->allocate_page(fd);
            assert(page_no != new_page_log->page_no_);

            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_NUM_PAGES, (char*)(&new_page_log->num_pages_), sizeof(int));
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FIRST_FREE_PAGE_NO, (char*)(&new_page_log->first_free_page_no_), sizeof(int));
            disk_manager_->update_value(fd, page_no, OFFSET_NEXT_FREE_PAGE_NO, (char*)(&new_page_log->next_free_page_no_), sizeof(int));
        } break;
        default:
        break;
    }
}

/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
int LogReplay::read_log(char *log_data, int size, int offset) {
    // read log file from the previous end
    assert (log_replay_fd_ != -1);
    int file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
    if (offset > file_size) {
        return -1;
    }

    size = std::min(size, file_size - offset);
    if(size == 0) return 0;
    lseek(log_replay_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_replay_fd_, log_data, size);
    assert(bytes_read == size);
    return bytes_read;
}

void LogReplay::replayFun(){
    int offset = persist_off_;
    int read_bytes;
    while (!replay_stop) {
        int read_size = std::min(max_replay_off_ - offset, (size_t)LOG_REPLAY_BUFFER_SIZE);
        if(read_size == 0){
            // don't need to replay
            std::this_thread::sleep_for(std::chrono::milliseconds(50)); //sleep 50 ms
            break;
        }
        read_bytes = read_log(buffer_.buffer_, read_size, offset);
        buffer_.offset_ = read_bytes - 1;
        int inner_offset = 0;
        int replay_batch_id;
        while (inner_offset <= buffer_.offset_ ) {

            if (inner_offset + OFFSET_LOG_TOT_LEN + sizeof(uint32_t) > LOG_REPLAY_BUFFER_SIZE) break;
            uint32_t size = *reinterpret_cast<const uint32_t *>(buffer_.buffer_ + inner_offset + OFFSET_LOG_TOT_LEN);
            if (size == 0 || size + inner_offset > LOG_REPLAY_BUFFER_SIZE) {
                break;
            }
            
            LogRecord *record;
            LogType type = *reinterpret_cast<const LogType *>(buffer_.buffer_ + inner_offset);
            switch (type)
            {
            case LogType::BEGIN:
                record = new BeginLogRecord();
                break;
            case LogType::ABORT:
                record = new AbortLogRecord();
                break;
            case LogType::COMMIT:
                record = new CommitLogRecord();
                break;
            case LogType::INSERT:
                record = new InsertLogRecord();
                break;
            case LogType::UPDATE:
                record = new UpdateLogRecord();
                break;
            case LogType::DELETE:
                record = new DeleteLogRecord();
                break;
            case LogType::NEWPAGE:
                record = new NewPageLogRecord();
            default:
                assert(0);
                break;
            }
            record->deserialize(buffer_.buffer_ + inner_offset);
            // redo the log if necessary
            apply_sigle_log(record);
            replay_batch_id = record->log_batch_id_;
            delete record;
            inner_offset += size;
        }
        offset += inner_offset;

        persist_batch_id_ = replay_batch_id;
        persist_off_ = offset;
        // 持久化persist_batch_id和persist_off
        lseek(log_write_head_fd_, 0, SEEK_SET);
        ssize_t result = write(log_write_head_fd_, &persist_batch_id_, sizeof(batch_id_t));
        if (result == -1) {
            std::cerr << "bad write\n";
        }

        result = write(log_write_head_fd_, &persist_off_, sizeof(uint64_t));
        if (result == -1) {
            std::cerr << "bad write\n";
        }
    }
}