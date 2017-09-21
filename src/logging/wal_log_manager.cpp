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

#include "logging/wal_log_manager.h"
#include "logging/durability_factory.h"
#include "catalog/manager.h"
#include "common/container_tuple.h"
#include "logging/logging_util.h"
#include "storage/tile_group.h"

namespace peloton {
namespace logging {

void WalLogManager::WriteRecordToBuffer(LogRecord &record) {
  if(likely_branch(is_running_)){

  // Reset the output buffer
  output_buffer_.Reset();

  // Reserve for the frame length
  size_t start = output_buffer_.Position();
  output_buffer_.WriteInt(0);

  LogRecordType type = record.GetType();
  output_buffer_.WriteEnumInSingleByte(static_cast<std::underlying_type_t<LogRecordType>>(type));

  switch (type) {
    case LogRecordType::TUPLE_INSERT:
     {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();
      std::vector<catalog::Column> columns;

      output_buffer_.WriteLong(record.GetCommitId());
      // Write down the database id and the table id
      output_buffer_.WriteLong(tg->GetDatabaseId());
      output_buffer_.WriteLong(tg->GetTableId());

      output_buffer_.WriteLong(tuple_pos.block);
      output_buffer_.WriteLong(tuple_pos.offset);


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
        val.SerializeTo(output_buffer_);
      }

      break;
    }
  case LogRecordType::TUPLE_DELETE:
  {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();

      output_buffer_.WriteLong(record.GetCommitId());
      // Write down the database id and the table id
      output_buffer_.WriteLong(tg->GetDatabaseId());
      output_buffer_.WriteLong(tg->GetTableId());

      output_buffer_.WriteLong(tuple_pos.block);
      output_buffer_.WriteLong(tuple_pos.offset);


      break;
  }
  case LogRecordType::TUPLE_UPDATE:
  {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto old_tuple_pos = record.GetOldItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();
      std::vector<catalog::Column> columns;

      output_buffer_.WriteLong(record.GetCommitId());
      // Write down the database id and the table id
      output_buffer_.WriteLong(tg->GetDatabaseId());
      output_buffer_.WriteLong(tg->GetTableId());

      output_buffer_.WriteLong(old_tuple_pos.block);
      output_buffer_.WriteLong(old_tuple_pos.offset);

      output_buffer_.WriteLong(tuple_pos.block);
      output_buffer_.WriteLong(tuple_pos.offset);
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
        val.SerializeTo(output_buffer_);
      }


      break;
  }
    case LogRecordType::TRANSACTION_BEGIN: {
      output_buffer_.WriteLong(record.GetCommitId());
      break;
  }
    case LogRecordType::TRANSACTION_COMMIT: {
      output_buffer_.WriteLong(record.GetCommitId());
      break;
    }
    default: {
      LOG_ERROR("Unsupported log record type");
      PL_ASSERT(false);
    }
  }

  // Add the frame length
  // XXX: We rely on the fact that the serializer treat a int32_t as 4 bytes
  int32_t length = output_buffer_.Position() - start - sizeof(int32_t);
  output_buffer_.WriteIntAt(start, length);

  // Copy the output buffer into current buffer
  bool is_success = buffer_ptr_->WriteData(output_buffer_.Data(), output_buffer_.Size());
  if (is_success == false) {
    // A buffer is full, pass it to the front end logger
    // Get a new buffer
    logger_->PersistLogBuffer(buffer_ptr_);
    buffer_ptr_ = new LogBuffer();
    // Write it again
    is_success = buffer_ptr_->WriteData(output_buffer_.Data(), output_buffer_.Size());
    PL_ASSERT(is_success);
  }
  }
}


void WalLogManager::StartPersistTxn(cid_t current_cid) {
  // Log down the begin of a transaction
  LogRecord record = LogRecordFactory::CreateTxnRecord(LogRecordType::TRANSACTION_BEGIN, current_cid);
  WriteRecordToBuffer(record);
}

void WalLogManager::EndPersistTxn(cid_t current_cid) {
  LogRecord record = LogRecordFactory::CreateTxnRecord(LogRecordType::TRANSACTION_COMMIT, current_cid);
  WriteRecordToBuffer(record);
}


void WalLogManager::LogInsert(const ItemPointer &tuple_pos, cid_t current_cid) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LogRecordType::TUPLE_INSERT, tuple_pos, current_cid);
  WriteRecordToBuffer(record);
  logger_->PersistLogBuffer(buffer_ptr_);
}

void WalLogManager::LogUpdate(const ItemPointer &tuple_old_pos, const ItemPointer &tuple_pos, cid_t current_cid) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LogRecordType::TUPLE_UPDATE, tuple_pos, current_cid);
  record.SetOldItemPointer(tuple_old_pos);
  WriteRecordToBuffer(record);
  logger_->PersistLogBuffer(buffer_ptr_);
}

void WalLogManager::LogDelete(const ItemPointer &tuple_pos_deleted, cid_t current_cid) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LogRecordType::TUPLE_DELETE, tuple_pos_deleted,current_cid);
  WriteRecordToBuffer(record);
  logger_->PersistLogBuffer(buffer_ptr_);
}

void WalLogManager::DoRecovery(){
    logger_->StartRecovery();
    logger_->WaitForRecovery();
 }

void WalLogManager::StartLoggers() {
  logger_->SetLogBuffer(buffer_ptr_);
  logger_->StartLogging();
  is_running_ = true;
}

void WalLogManager::StopLoggers() {
  logger_->StopLogging();
  is_running_ = false;

}

}
}
