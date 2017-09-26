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
  CopySerializeOutput* output_buffer = new CopySerializeOutput();

  // Reserve for the frame length
  size_t start = output_buffer->Position();
  output_buffer->WriteInt(0);

  LogRecordType type = record.GetType();
  output_buffer->WriteEnumInSingleByte(static_cast<std::underlying_type_t<LogRecordType>>(type));

  output_buffer->WriteLong(record.GetEpochId());
  output_buffer->WriteLong(record.GetCommitId());

  switch (type) {
    case LogRecordType::TUPLE_INSERT:
     {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();
      std::vector<catalog::Column> columns;
      // Write down the database id and the table id
      output_buffer->WriteLong(tg->GetDatabaseId());
      output_buffer->WriteLong(tg->GetTableId());

      output_buffer->WriteLong(tuple_pos.block);
      output_buffer->WriteLong(tuple_pos.offset);

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
        val.SerializeTo(*(output_buffer));
      }

      break;
    }
  case LogRecordType::TUPLE_DELETE:
  {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();

      // Write down the database id and the table id
      output_buffer->WriteLong(tg->GetDatabaseId());
      output_buffer->WriteLong(tg->GetTableId());

      output_buffer->WriteLong(tuple_pos.block);
      output_buffer->WriteLong(tuple_pos.offset);


      break;
  }
  case LogRecordType::TUPLE_UPDATE:
  {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto old_tuple_pos = record.GetOldItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();
      std::vector<catalog::Column> columns;

      // Write down the database id and the table id
      output_buffer->WriteLong(tg->GetDatabaseId());
      output_buffer->WriteLong(tg->GetTableId());

      output_buffer->WriteLong(old_tuple_pos.block);
      output_buffer->WriteLong(old_tuple_pos.offset);

      output_buffer->WriteLong(tuple_pos.block);
      output_buffer->WriteLong(tuple_pos.offset);
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
        val.SerializeTo(*(output_buffer));
      }


      break;
  }
    case LogRecordType::TRANSACTION_BEGIN: {
      output_buffer->WriteLong(record.GetCommitId());
      break;
  }
    case LogRecordType::TRANSACTION_COMMIT: {
      output_buffer->WriteLong(record.GetCommitId());
      break;
    }
    default: {
      LOG_ERROR("Unsupported log record type");
      PL_ASSERT(false);
    }
  }

  // Add the frame length
  // XXX: We rely on the fact that the serializer treat a int32_t as 4 bytes
  int32_t length = output_buffer->Position() - start - sizeof(int32_t);
  output_buffer->WriteIntAt(start, length);
  logger_->PushBuffer(output_buffer);

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


void WalLogManager::LogInsert(const ItemPointer &tuple_pos, cid_t current_cid, eid_t current_eid) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LogRecordType::TUPLE_INSERT, tuple_pos, current_cid, current_eid);
  WriteRecordToBuffer(record);
  //buffer_ptr_ = logger_->PersistLogBuffer(std::move(buffer_ptr_));
}

void WalLogManager::LogUpdate(const ItemPointer &tuple_old_pos, const ItemPointer &tuple_pos, cid_t current_cid, eid_t current_eid) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LogRecordType::TUPLE_UPDATE, tuple_pos, current_cid, current_eid);
  record.SetOldItemPointer(tuple_old_pos);
  WriteRecordToBuffer(record);
  //logger_->PersistLogBuffer(std::move(buffer_ptr_));
}

void WalLogManager::LogDelete(const ItemPointer &tuple_pos_deleted, cid_t current_cid, eid_t current_eid) {
  LogRecord record = LogRecordFactory::CreateTupleRecord(LogRecordType::TUPLE_DELETE, tuple_pos_deleted,current_cid, current_eid);
  WriteRecordToBuffer(record);
  //logger_->PersistLogBuffer(std::move(buffer_ptr_));
}

void WalLogManager::DoRecovery(){
    logger_->StartRecovery();
    logger_->WaitForRecovery();
 }

void WalLogManager::StartLoggers() {
  logger_->StartLogging();
  is_running_ = true;
}

void WalLogManager::StopLoggers() {
  logger_->StopLogging();
  is_running_ = false;

}

}
}
