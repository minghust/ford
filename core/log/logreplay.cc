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
            disk_manager_->update_value(fd, insert_log->rid_.page_no_, insert_log->rid_.slot_offset_, insert_log->insert_value_.value_, insert_log->insert_value_.value_size_ * sizeof(char));
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
            disk_manager_->update_value(fd, update_log->rid_.page_no_, update_log->rid_.slot_offset_, update_log->new_value_.value_, update_log->new_value_.value_size_ * sizeof(char));
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