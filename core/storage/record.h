#pragma once

#include <memory.h>

#include "base/common.h"

#define PAGE_NO_RM_FILE_HDR 0
#define OFFSET_PAGE_HDR 0
#define OFFSET_FIRST_FREE_PAGE_NO 12
#define OFFSET_NUM_RECORDS 4
#define OFFSET_NEXT_FREE_PAGE_NO 0
#define OFFSET_BITMAP 8

class RmFileHdr {
    int record_size_;
    int num_pages_;
    int num_records_per_page_;
    int first_free_page_no_;
    int bitmap_size_;
};

class RmPageHdr {
    int next_free_page_no_;
    int num_records_;
};

class Rid {
public:
    int page_no_;
    int slot_offset_;
};

class RmRecord {
public:
    itemkey_t key_;
    size_t value_size_;
    char* value_;
    bool allocated_ = false;

    RmRecord() = default;

    RmRecord(const RmRecord& other) {
        key_ = other.key_;
        value_size_ = other.value_size_;
        value_ = new char[value_size_];
        memcpy(value_, other.value_, value_size_);
        allocated_ = true;
    }

    RmRecord& operator = (const RmRecord& other) {
        key_ = other.key_;
        value_size_ = other.value_size_;
        value_ = new char[value_size_];
        memcpy(value_, other.value_, value_size_);
        allocated_ = true;
        return *this;
    }

    void Deserialize(const char* data) {
        key_ = *reinterpret_cast<const itemkey_t*>(data);
        value_size_ = *reinterpret_cast<const int*>(data);
        if(allocated_) {
            delete[] value_;
        }
        value_ = new char[value_size_];
        memcpy(value_, data + sizeof(int) + sizeof(itemkey_t), value_size_);
    }

    ~RmRecord() {
        if(allocated_) {
            delete[] value_;
        }
        allocated_ = false;
        value_ = nullptr;
    }
};