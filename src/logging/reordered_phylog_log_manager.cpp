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
void ReorderedPhyLogLogManager::RegisterWorker() {
  PL_ASSERT(tl_worker_ctx == nullptr);
  // shuffle worker to logger
  tl_worker_ctx = new WorkerContext(worker_count_++);
  size_t logger_id = HashToLogger(tl_worker_ctx->worker_id);

  loggers_[logger_id]->RegisterWorker(tl_worker_ctx);
}

// deregister worker threads.
void ReorderedPhyLogLogManager::DeregisterWorker() {
  PL_ASSERT(tl_worker_ctx != nullptr);

  size_t logger_id = HashToLogger(tl_worker_ctx->worker_id);

  loggers_[logger_id]->DeregisterWorker(tl_worker_ctx);
}

void ReorderedPhyLogLogManager::WriteRecordToBuffer(LogRecord &record) {
  WorkerContext *ctx = tl_worker_ctx;
  LOG_TRACE("Worker %d write a record", ctx->worker_id);

  PL_ASSERT(ctx);

  // First serialize the epoch to current output buffer
  // TODO: Eliminate this extra copy
  auto &output = ctx->output_buffer;

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

      expression::ContainerTuple<storage::TileGroup> container_tuple(
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
      output.WriteLong(ctx->current_cid);
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

  size_t epoch_idx = ctx->current_commit_eid % concurrency::EpochManager::GetEpochQueueCapacity();

  PL_ASSERT(ctx->per_epoch_buffer_ptrs[epoch_idx].empty() == false);
  LogBuffer* buffer_ptr = ctx->per_epoch_buffer_ptrs[epoch_idx].top().get();
  PL_ASSERT(buffer_ptr);

  // Add the frame length
  // XXX: We rely on the fact that the serializer treat a int32_t as 4 bytes
  int32_t length = output.Position() - start - sizeof(int32_t);
  output.WriteIntAt(start, length);

  // Copy the output buffer into current buffer
  bool is_success = buffer_ptr->WriteData(output.Data(), output.Size());
  if (is_success == false) {
    // A buffer is full, pass it to the front end logger
    // Get a new buffer and register it to current epoch
    buffer_ptr = RegisterNewBufferToEpoch(std::move((ctx->buffer_pool.GetBuffer(ctx->current_commit_eid))));
    // Write it again
    is_success = buffer_ptr->WriteData(output.Data(), output.Size());
    PL_ASSERT(is_success);
  }
}


void ReorderedPhyLogLogManager::StartPersistTxn() {
  // Log down the begin of a transaction
  LogRecord record = LogRecordFactory::CreateTxnRecord(LogRecordType::TRANSACTION_BEGIN, tl_worker_ctx->current_cid);
  WriteRecordToBuffer(record);
}

void ReorderedPhyLogLogManager::EndPersistTxn() {
  PL_ASSERT(tl_worker_ctx);
  LogRecord record = LogRecordFactory::CreateTxnRecord(LogRecordType::TRANSACTION_COMMIT, tl_worker_ctx->current_cid);
  WriteRecordToBuffer(record);
  tl_worker_ctx->current_commit_eid = MAX_EID;
}

void ReorderedPhyLogLogManager::StartTxn(concurrency::Transaction *txn) {
  PL_ASSERT(tl_worker_ctx);
  size_t cur_eid = concurrency::EpochManagerFactory::GetInstance().GetCurrentEpochId();

  tl_worker_ctx->current_commit_eid = cur_eid;

  size_t epoch_idx = cur_eid % concurrency::EpochManager::GetEpochQueueCapacity();

  if (tl_worker_ctx->per_epoch_buffer_ptrs[epoch_idx].empty() == true) {
    RegisterNewBufferToEpoch(std::move(tl_worker_ctx->buffer_pool.GetBuffer(cur_eid)));
  }

  // Record the txn timer
  DurabilityFactory::StartTxnTimer(cur_eid, tl_worker_ctx);

  // Handle the commit id
  cid_t txn_cid = txn->GetCommitId();
  tl_worker_ctx->current_cid = txn_cid;
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

void ReorderedPhyLogLogManager::DoRecovery(const size_t &begin_eid){
  size_t end_eid = RecoverPepoch();
  // printf("recovery_thread_count = %d, logger count = %d\n", (int)recovery_thread_count_, (int)logger_count_);
  size_t recovery_thread_per_logger = (size_t) ceil(recovery_thread_count_ * 1.0 / logger_count_);

  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    LOG_TRACE("Start logger %d for recovery", (int) logger_id);
    // TODO: properly set this two eid
    loggers_[logger_id]->StartRecovery(begin_eid, end_eid, recovery_thread_per_logger);
  }

  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    loggers_[logger_id]->WaitForRecovery();
  }

  // Rebuild all secondary indexes
  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    loggers_[logger_id]->StartIndexRebulding(logger_count_);
  }

  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    loggers_[logger_id]->WaitForIndexRebuilding();
  }

  // Reset the epoch manager
  concurrency::EpochManagerFactory::GetInstance().Reset(end_eid + 1);
}

void ReorderedPhyLogLogManager::StartLoggers() {
  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    LOG_TRACE("Start logger %d", (int) logger_id);
    loggers_[logger_id]->StartLogging();
  }
  is_running_ = true;
  pepoch_thread_.reset(new std::thread(&ReorderedPhyLogLogManager::RunPepochLogger, this));
}

void ReorderedPhyLogLogManager::StopLoggers() {
  for (size_t logger_id = 0; logger_id < logger_count_; ++logger_id) {
    loggers_[logger_id]->StopLogging();
  }
  is_running_ = false;
  pepoch_thread_->join();
}

void ReorderedPhyLogLogManager::RunPepochLogger() {

  FileHandle file_handle;
  std::string filename = pepoch_dir_ + "/" + pepoch_filename_;
  // Create a new file
  if (LoggingUtil::OpenFile(filename.c_str(), "wb", file_handle) == false) {
    LOG_ERROR("Unable to create pepoch file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }


  while (true) {
    if (is_running_ == false) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::microseconds
                                  (concurrency::EpochManagerFactory::GetInstance().GetEpochLengthInMicroSecQuarter())
    );

    size_t curr_persist_epoch_id = MAX_EID;
    for (auto &logger : loggers_) {
      size_t logger_pepoch_id = logger->GetPersistEpochId();
      if (logger_pepoch_id < curr_persist_epoch_id) {
        curr_persist_epoch_id = logger_pepoch_id;
      }
    }

    PL_ASSERT(curr_persist_epoch_id < MAX_EID);
    size_t glob_peid = global_persist_epoch_id_.load();
    if (curr_persist_epoch_id > glob_peid) {
      // we should post the pepoch id after the fsync -- Jiexi
      fwrite((const void *) (&curr_persist_epoch_id), sizeof(curr_persist_epoch_id), 1, file_handle.file);
      global_persist_epoch_id_ = curr_persist_epoch_id;
      // printf("global persist epoch id = %d\n", (int)global_persist_epoch_id_);
      // Call fsync
      LoggingUtil::FFlushFsync(file_handle);
    }
  }

  // Safely close the file
  bool res = LoggingUtil::CloseFile(file_handle);
  if (res == false) {
    LOG_ERROR("Cannot close pepoch file");
    exit(EXIT_FAILURE);
  }

}

size_t ReorderedPhyLogLogManager::RecoverPepoch() {
  FileHandle file_handle;
  std::string filename = pepoch_dir_ + "/" + pepoch_filename_;
  // Create a new file
  if (LoggingUtil::OpenFile(filename.c_str(), "rb", file_handle) == false) {
    LOG_ERROR("Unable to open pepoch file %s\n", filename.c_str());
    exit(EXIT_FAILURE);
  }

  size_t persist_epoch_id = 0;

  while (true) {
    if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) &persist_epoch_id, sizeof(persist_epoch_id)) == false) {
      LOG_TRACE("Reach the end of the log file");
      break;
    }
    //printf("persist_epoch_id = %d\n", (int)persist_epoch_id);
  }

  // Safely close the file
  bool res = LoggingUtil::CloseFile(file_handle);
  if (res == false) {
    LOG_ERROR("Cannot close pepoch file");
    exit(EXIT_FAILURE);
  }

  return persist_epoch_id;
}

}
}
