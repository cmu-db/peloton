//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// reordered_phylog_logger.cpp
//
// Identification: src/backend/logging/reordered_phylog_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <algorithm>
#include <dirent.h>
#include <cstdio>
#include "gc/gc_manager_factory.h"
#include "concurrency/transaction_manager_factory.h"
#include "index/index_factory.h"

#include "catalog/catalog_defaults.h"
#include "catalog/column.h"
#include "catalog/manager.h"
#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/database_catalog.h"
#include "concurrency/epoch_manager_factory.h"
#include "common/container_tuple.h"
#include "logging/logging_util.h"
#include "storage/tile_group.h"
#include "storage/database.h"
#include "storage/tile_group_header.h"
#include "type/ephemeral_pool.h"
#include "storage/storage_manager.h"

#include "logging/wal_logger.h"
#include "logging/wal_log_manager.h"

namespace peloton {
namespace logging {


void WalLogger::WriteTransaction(std::vector<LogRecord> log_records, ResultType* status){
LogBuffer* buf = new LogBuffer();
for(LogRecord record : log_records){
    CopySerializeOutput* output = WriteRecordToBuffer(record);
    if(!buf->WriteData(output->Data(),output->Size())){
        PersistLogBuffer(buf);
        buf = new LogBuffer();
    }
    delete output;
}
if(!buf->Empty()){
    PersistLogBuffer(buf);
}
*status = ResultType::SUCCESS;
}

CopySerializeOutput* WalLogger::WriteRecordToBuffer(LogRecord &record) {
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
  return output_buffer;

}
void WalLogger::PersistLogBuffer(LogBuffer* log_buffer) {
    FileHandle *new_file_handle = new FileHandle();
    if(likely_branch(log_buffer != nullptr)){
    std::string filename = GetLogFileFullPath(0);
    // Create a new file
    if (LoggingUtil::OpenFile(filename.c_str(), "ab", *new_file_handle) == false) {
      LOG_ERROR("Unable to create log file %s\n", filename.c_str());
      exit(EXIT_FAILURE);
    }

    fwrite((const void *) (log_buffer->GetData()), log_buffer->GetSize(), 1, new_file_handle->file);
    delete log_buffer;

//  Call fsync
    LoggingUtil::FFlushFsync(*new_file_handle);

    bool res = LoggingUtil::CloseFile(*new_file_handle);
    if (res == false) {
      LOG_ERROR("Cannot close log file under directory %s", log_dir_.c_str());
      exit(EXIT_FAILURE);
    }
}
    delete new_file_handle;

}

}}
