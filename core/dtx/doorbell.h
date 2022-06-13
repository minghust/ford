// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include "base/common.h"
#include "rlib/rdma_ctrl.hpp"
#include "scheduler/corotine_scheduler.h"

using namespace rdmaio;

// Two RDMA requests are sent to the QP in a doorbelled (or batched) way.
// These requests are executed within one round trip
// Target: improve performance

class DoorbellBatch {
 public:
  DoorbellBatch() {
    // The key of doorbell: set the pointer to link two requests
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];
    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = IBV_SEND_SIGNALED;
    sr[1].next = NULL;
  }

  struct ibv_send_wr sr[2];

  struct ibv_sge sge[2];

  struct ibv_send_wr* bad_sr;
};

class LockReadBatch : public DoorbellBatch {
 public:
  LockReadBatch() : DoorbellBatch() {}

  // SetLockReq and SetReadReq are a doorbelled group
  // First lock, then read
  void SetLockReq(char* local_addr, uint64_t remote_off, uint64_t compare, uint64_t swap);

  void SetReadReq(char* local_addr, uint64_t remote_off, size_t size);

  // Send doorbelled requests to the queue pair
  bool SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id);

  // Fill the parameters
  bool FillParams(RCQP* qp);
};

class WriteUnlockBatch : public DoorbellBatch {
 public:
  WriteUnlockBatch() : DoorbellBatch() {}

  // SetWritePrimaryReq and SetUnLockReq are a doorbelled group
  // First write, then unlock
  void SetWritePrimaryReq(char* local_addr, uint64_t remote_off, size_t size);

  void SetUnLockReq(char* local_addr, uint64_t remote_off);

  void SetUnLockReq(char* local_addr, uint64_t remote_off, uint64_t compare, uint64_t swap);

  // Send doorbelled requests to the queue pair
  bool SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, int use_cas);
};

class InvisibleWriteBatch : public DoorbellBatch {
 public:
  InvisibleWriteBatch() : DoorbellBatch() {}

  // SetInvisibleReq and SetWriteRemoteReq are a doorbelled group
  // First lock, then write
  void SetInvisibleReq(char* local_addr, uint64_t remote_off, uint64_t compare, uint64_t swap);

  void SetInvisibleReq(char* local_addr, uint64_t remote_off);

  void SetWriteRemoteReq(char* local_addr, uint64_t remote_off, size_t size);

  // Send doorbelled requests to the queue pair
  bool SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, int use_cas);

  bool SendReqsSync(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, int use_cas);
};

class WriteFlushBatch : public DoorbellBatch {
 public:
  WriteFlushBatch() : DoorbellBatch() {}

  void SetWriteRemoteReq(char* local_addr, uint64_t remote_off, size_t size);

  void SetReadRemoteReq(char* local_addr, uint64_t remote_off, size_t size);
  // Send doorbelled requests to the queue pair
  bool SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, MemoryAttr& remote_mr);
};

class InvisibleWriteFlushBatch {
 public:
  InvisibleWriteFlushBatch() {
    // The key of doorbell: set the pointer to link two requests
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = 0;
    sr[1].next = &sr[2];

    sr[2].num_sge = 1;
    sr[2].sg_list = &sge[2];
    sr[2].send_flags = IBV_SEND_SIGNALED;
    sr[2].next = NULL;
  }

  void SetInvisibleReq(char* local_addr, uint64_t remote_off);

  void SetWriteRemoteReq(char* local_addr, uint64_t remote_off, size_t size);

  void SetReadRemoteReq(char* local_addr, uint64_t remote_off, size_t size);

  // Send doorbelled requests to the queue pair
  bool SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, int use_cas);

 private:
  struct ibv_send_wr sr[3];

  struct ibv_sge sge[3];

  struct ibv_send_wr* bad_sr;
};

class ComparatorUpdateRemote {
 public:
  ComparatorUpdateRemote() {
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = 0;
    sr[1].next = &sr[2];

    sr[2].num_sge = 1;
    sr[2].sg_list = &sge[2];
    sr[2].send_flags = IBV_SEND_SIGNALED;
    sr[2].next = NULL;
  }

  void SetInvisibleReq(char* local_addr, uint64_t remote_off);

  void SetWriteRemoteReq(char* local_addr, uint64_t remote_off, size_t size);

  void SetReleaseReq(char* local_addr, uint64_t remote_off);

  // Send doorbelled requests to the queue pair
  bool SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, int use_cas);

 private:
  struct ibv_send_wr sr[3];

  struct ibv_sge sge[3];

  struct ibv_send_wr* bad_sr;
};