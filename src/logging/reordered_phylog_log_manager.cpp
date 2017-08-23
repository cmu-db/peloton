//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// reordered_phylog_log_manager.cpp
//
// Identification: src/backend/logging/reordered_phylog_log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "concurrency/epoch_manager_factory.h"
#include "logging/reordered_phylog_log_manager.h"
#include "logging/durability_factory.h"
#include "catalog/manager.h"
#include "common/container_tuple.h"
#include "logging/logging_util.h"
#include "storage/tile_group.h"

namespace peloton {
namespace logging {

// register worker threads to the log manager before execution.
// note that we always construct logger prior to worker.
// this function is called by each worker thread.

/*void ReorderedPhyLogLogManager::RegisterWorker(size_t thread_id) {
  PL_ASSERT(tl_worker_ctx == nullptr);
  //  shuffle worker to logger
  tl_worker_ctx = new WorkerContext(worker_count_++, thread_id);
  size_t logger_id = HashToLogger(tl_worker_ctx->worker_id);

  loggers_[0]->RegisterWorker();
}*/

// deregister worker threads.
/*void ReorderedPhyLogLogManager::DeregisterWorker() {
  PL_ASSERT(tl_worker_ctx != nullptr);

  size_t logger_id = HashToLogger(tl_worker_ctx->worker_id);

  loggers_[logger_id]->DeregisterWorker(tl_worker_ctx);
}*/

void ReorderedPhyLogLogManager::WriteRecordToBuffer(LogRecord &record) {
//  WorkerContext *ctx = tl_worker_ctx;
//  LOG_DEBUG("Worker %d write a record", ctx->worker_id);

//  PL_ASSERT(ctx);

  // First serialize the epoch to current output buffer
  // TODO: Eliminate this extra copy
  if(likely_branch(is_running_)){
    auto &output = output_buffer_;

  // Reset the output buffer
  output.Reset();

  // Reserve for the frame length
  size_t start = output.Position();
  output.WriteInt(0);

  LogRecordType type = record.GetType();
  output.WriteEnumInSingleByte(static_cast<std::underlying_type_t<LogRecordType>>(type));

  switch (type) {
    case LogRecordType::TUPLE_INSERT:
    case LogRecordType::TUPLE_DELETE:
    case LogRecordType::TUPLE_UPDATE: {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();
      std::vector<catalog::Column> columns;

      // Write down the database id and the table id
      output.WriteLong(tg->GetDatabaseId());
      output.WriteLong(tg->GetTableId());

      // size_t start = output.Position();
      // output.WriteInt(0);

      // Write the full tuple into the buffer
      for(auto schema : tg->GetTileSchemas()){
          for(auto column : schema.GetColumns()){
              columns.push_back(column);
          }
      }

      ContainerTuple<storage::TileGroup> container_tuple(
        tg, tuple_pos.offset
      );
      for(oid_t oid = 0; oid < columns.size(); oid++){
        auto val = container_tuple.GetValue(oid);
        val.SerializeTo(output);
      }
      // output.WriteIntAt(start, (int32_t)output.Size());

      break;
    }
    case LogRecordType::TRANSACTION_BEGIN:
    case LogRecordType::TRANSACTION_COMMIT: {
      output.WriteLong(record.GetCommitId());
      break;
    }
      // case LOGRECORD_TYPE_EPOCH_BEGIN:
      // case LOGRECORD_TYPE_EPOCH_END: {
      //   output.WriteLong((uint64_t) ctx->current_eid);
      //   break;
      // }
    default: {
      LOG_ERROR("Unsupported log record type");
      PL_ASSERT(false);
    }
  }

//  size_t epoch_idx = ctx->current_commit_eid % concurrency::EpochManager::GetEpochQueueCapacity();

//  PL_ASSERT(ctx->per_epoch_buffer_ptrs[epoch_idx].empty() == false);
 // LogBuffer* buffer_ptr = logging::LogBufferPool::GetBuffer();//ctx->per_epoch_buffer_ptrs[epoch_idx].top().get();
 // PL_ASSERT(buffer_ptr);

  // Add the frame length
  // XXX: We rely on the fact that the serializer treat a int32_t as 4 bytes
  int32_t length = output.Position() - start - sizeof(int32_t);
  output.WriteIntAt(start, length);

  // Copy the output buffer into current buffer
  bool is_success = buffer_ptr_->WriteData(output.Data(), output.Size());
  if (is_success == false) {
    // A buffer is full, pass it to the front end logger
    // Get a new buffer and register it to current epoch
    logger_->PersistLogBuffer(buffer_ptr_);
    buffer_ptr_ = new LogBuffer();
    //buffer_ptr = RegisterNewBufferToEpoch(std::move(GetBuffer()));
    // Write it again
    is_success = buffer_ptr_->WriteData(output.Data(), output.Size());
    PL_ASSERT(is_success);
  }
  }
}


void ReorderedPhyLogLogManager::StartPersistTxn(cid_t current_cid) {
  // Log down the begin of a transaction
  LogRecord record = LogRecordFactory::CreateTxnRecord(LogRecordType::TRANSACTION_BEGIN, current_cid);
  WriteRecordToBuffer(record);
}

void ReorderedPhyLogLogManager::EndPersistTxn(cid_t current_cid) {
//  PL_ASSERT(tl_worker_ctx);
  LogRecord record = LogRecordFactory::CreateTxnRecord(LogRecordType::TRANSACTION_COMMIT, current_cid);
  WriteRecordToBuffer(record);
//  tl_worker_ctx->current_commit_eid = MAX_EID;
}


void ReorderedPhyLogLogManager::LogInsert(const ItemPointer &tuple_pos) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LogRecordType::TUPLE_INSERT, tuple_pos);
  WriteRecordToBuffer(record);
}

void ReorderedPhyLogLogManager::LogUpdate(const ItemPointer &tuple_pos) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LogRecordType::TUPLE_UPDATE, tuple_pos);
  WriteRecordToBuffer(record);
}

void ReorderedPhyLogLogManager::LogDelete(const ItemPointer &tuple_pos_deleted) {
  // Need the tuple value for the deleted tuple
  LogRecord record = LogRecordFactory::CreateTupleRecord(LogRecordType::TUPLE_DELETE, tuple_pos_deleted);
  WriteRecordToBuffer(record);
}

void ReorderedPhyLogLogManager::DoRecovery(){
    logger_->StartRecovery();
    logger_->WaitForRecovery();
 }

void ReorderedPhyLogLogManager::StartLoggers() {
  logger_->SetLogBuffer(buffer_ptr_);
  logger_->StartLogging();
  is_running_ = true;
}

void ReorderedPhyLogLogManager::StopLoggers() {
  logger_->StopLogging();
  is_running_ = false;

}

}
}
