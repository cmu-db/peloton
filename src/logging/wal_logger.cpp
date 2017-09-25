//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// wal_logger.cpp
//
// Identification: src/logging/wal_logger.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <algorithm>
#include <dirent.h>
#include <cstdio>

#include "catalog/manager.h"
#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/database_catalog.h"
#include "common/container_tuple.h"
#include "logging/logging_util.h"
#include "storage/tile_group.h"

#include "logging/wal_logger.h"
#include "logging/wal_log_manager.h"

namespace peloton {
namespace logging {

// Gets all the records created on the txn rw set and logs them.
void WalLogger::WriteTransaction(std::vector<LogRecord> log_records) {
  LogBuffer *buf = new LogBuffer();
  for (LogRecord record : log_records) {
    CopySerializeOutput *output = WriteRecordToBuffer(record);
    if (!buf->WriteData(output->Data(), output->Size())) {
      PersistLogBuffer(buf);
      buf = new LogBuffer();
    }
    delete output;
  closedir(dirp);
  }
  if (!buf->Empty()) {
    PersistLogBuffer(buf);
  }
}

CopySerializeOutput *WalLogger::WriteRecordToBuffer(LogRecord &record) {
  // Reset the output buffer
  CopySerializeOutput *output_buffer = new CopySerializeOutput();

  // Reserve for the frame length
  size_t start = output_buffer->Position();
  output_buffer->WriteInt(0);

  LogRecordType type = record.GetType();
  output_buffer->WriteEnumInSingleByte(
      static_cast<std::underlying_type_t<LogRecordType>>(type));

  output_buffer->WriteLong(record.GetEpochId());
  output_buffer->WriteLong(record.GetCommitId());

  switch (type) {
    case LogRecordType::TUPLE_INSERT: {
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
      for (auto schema : tg->GetTileSchemas()) {
        for (auto column : schema.GetColumns()) {
          columns.push_back(column);
        }
      }
          eid_t record_eid = record_decode.ReadLong();
          if(record_eid > current_eid){
            current_eid =  record_eid;
          }

      ContainerTuple<storage::TileGroup> container_tuple(tg, tuple_pos.offset);
      for (oid_t oid = 0; oid < columns.size(); oid++) {
        auto val = container_tuple.GetValue(oid);
        val.SerializeTo(*(output_buffer));
      }

               //Catalog is already created, we must do this transactionally
                      catalog::TableCatalog::GetInstance()->GetNextOid();
                      if(index >= columns.size())
                      {
                          //Made to fit index as the last element
                          columns.resize(index+1);
                      }
                          columns[index] = tmp_col;
                        //  columns.insert(columns.begin(), catalog::Column();
                      catalog::ColumnCatalog::GetInstance()->GetNextOid();
      break;
    }
    case LogRecordType::TUPLE_DELETE: {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();
                  LOG_DEBUG("\n\n\nPG_INDEX\n\n\n");
                  indexes.push_back(std::move(tuple));
                  catalog::IndexCatalog::GetInstance()->GetNextOid();

              }
              //Simply insert the tuple in the tilegroup directly
              table->IncreaseTupleCount(1);
        if(record_eid > current_eid){
          current_eid =  record_eid;
        }

      // Write down the database id and the table id
      output_buffer->WriteLong(tg->GetDatabaseId());
      output_buffer->WriteLong(tg->GetTableId());

      output_buffer->WriteLong(tuple_pos.block);
      output_buffer->WriteLong(tuple_pos.offset);
                      auto db_oid = tg->GetValue(tg_offset, 2).GetAs<oid_t>();
                      auto table_oid = tg->GetValue(tg_offset, 0).GetAs<oid_t>();
              case INDEX_CATALOG_OID: //pg_index
                  {
                  std::vector<std::unique_ptr<storage::Tuple>>::iterator pos =
                  std::find_if(indexes.begin(), indexes.end(), [&tg, &tg_offset](const std::unique_ptr<storage::Tuple> &arg) {
                                                             return arg->GetValue(0).CompareEquals(tg->GetValue(tg_offset, 0)); });
                  if(pos != indexes.end())
                      indexes.erase(pos);
                  }


            tg->DeleteTupleFromRecovery(current_cid, tg_offset);
            table->IncreaseTupleCount(1);
      break;
    }
    case LogRecordType::TUPLE_UPDATE: {
      auto &manager = catalog::Manager::GetInstance();
      auto tuple_pos = record.GetItemPointer();
      auto old_tuple_pos = record.GetOldItemPointer();
      auto tg = manager.GetTileGroup(tuple_pos.block).get();
      std::vector<catalog::Column> columns;
        //The required tile group might not have been created yet
        if(tg.get() == nullptr)
        {
          table->AddTileGroupWithOidForRecovery(tg_block);
          tg = table->GetTileGroupById(tg_block);
        }

      // Write down the database id and the table id
      output_buffer->WriteLong(tg->GetDatabaseId());
      output_buffer->WriteLong(tg->GetTableId());

      output_buffer->WriteLong(old_tuple_pos.block);
      output_buffer->WriteLong(old_tuple_pos.offset);
      output_buffer->WriteLong(tuple_pos.block);
      output_buffer->WriteLong(tuple_pos.offset);
      // Write the full tuple into the buffer
      for (auto schema : tg->GetTileSchemas()) {
        for (auto column : schema.GetColumns()) {
          columns.push_back(column);
        }
      }

      ContainerTuple<storage::TileGroup> container_tuple(tg, tuple_pos.offset);
      for (oid_t oid = 0; oid < columns.size(); oid++) {
        auto val = container_tuple.GetValue(oid);
        val.SerializeTo(*(output_buffer));
      }
      epoch_manager.StartEpoch();
  std::set<storage::DataTable*> tables_with_indexes;

  //Index construction that was deferred from the read records phase
  for(auto const& tup : indexes){
      std::vector<oid_t> key_attrs;
      oid_t key_attr;
      auto table_catalog = catalog::TableCatalog::GetInstance();
      auto txn = concurrency::TransactionManagerFactory::GetInstance().BeginTransaction(IsolationLevelType::SERIALIZABLE);
      auto table_object = table_catalog->GetTableObject(tup->GetValue(2).GetAs<oid_t>(),txn);
      auto database_oid = table_object->database_oid;
      auto table = storage::StorageManager::GetInstance()->GetTableWithOid(database_oid, table_object->table_oid);
      concurrency::TransactionManagerFactory::GetInstance().CommitTransaction(txn);
      auto tuple_schema = table->GetSchema();
      std::stringstream iss( tup->GetValue(6).ToString() );
      while ( iss >> key_attr )
        key_attrs.push_back( key_attr );
      auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
      key_schema->SetIndexedColumns(key_attrs);

     index::IndexMetadata* index_metadata = new index::IndexMetadata(tup->GetValue(1).ToString(),
                                                                     tup->GetValue(0).GetAs<oid_t>(),
                                                                     table->GetOid(),
                                                                     database_oid,
                                                                     static_cast<IndexType>(tup->GetValue(3).GetAs<oid_t>()),
                                                                     static_cast<IndexConstraintType>(tup->GetValue(4).GetAs<oid_t>()),
                                                                     tuple_schema,
                                                                     key_schema,
                                                                     key_attrs,
                                                                     tup->GetValue(4).GetAs<bool>());
        std::shared_ptr<index::Index> index(
         index::IndexFactory::GetIndex(index_metadata));
        table->AddIndex(index);
        tables_with_indexes.insert(table);
      //Attributes must be changed once we have arraytype
  }


  //add tuples to index

  for (storage::DataTable* table : tables_with_indexes) {
      auto schema = table->GetSchema();
      size_t tg_count = table->GetTileGroupCount();
      for(oid_t tg = 0; tg<tg_count; tg++){
          auto tile_group = table->GetTileGroup(tg);
          for(oid_t offset = 0; offset < tile_group->GetActiveTupleCount(); offset++){
            storage::Tuple* t = new storage::Tuple(schema, true);
            for(size_t col = 0; col < schema->GetColumnCount(); col++){
                t->SetValue(col, tile_group->GetValue(offset, col));
            }
            ItemPointer p(tile_group->GetTileGroupId(), offset);
           // auto txn = concurrency::TransactionManagerFactory::GetInstance().BeginTransaction(IsolationLevelType::SERIALIZABLE);
            ItemPointer* i = new ItemPointer(tg, offset);
            table->InsertInIndexes(t, p, nullptr, &i);
           // concurrency::TransactionManagerFactory::GetInstance().CommitTransaction(txn);
          }

      }

  }

      break;
    }
    default: {
      LOG_ERROR("Unsupported log record type");
      PL_ASSERT(false);
      return nullptr;
    }
  }

  // Add the frame length
  // XXX: We rely on the fact that the serializer treat a int32_t as 4 bytes
  int32_t length = output_buffer->Position() - start - sizeof(int32_t);
  output_buffer->WriteIntAt(start, length);
  return output_buffer;
void WalLogger::PersistLogBuffer(LogBuffer *log_buffer) {
  FileHandle *new_file_handle = new FileHandle();
  if (likely_branch(log_buffer != nullptr)) {
    std::string filename = GetLogFileFullPath(0);
    // Create a new file
    if (LoggingUtil::OpenFile(filename.c_str(), "ab", *new_file_handle) ==
        false) {
      LOG_ERROR("Unable to create log file %s\n", filename.c_str());
      exit(EXIT_FAILURE);
    }

    fwrite((const void *)(log_buffer->GetData()), log_buffer->GetSize(), 1,
           new_file_handle->file);
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
void WalLogger::Run() {
   /**
    *  Main loop
    */
   while (true) {
     if (is_running_ == false) { break; }
     {
       /* if(log_buffer_ != nullptr && !log_buffer_->Empty()){
          log_buffers_.push_back(log_buffer_);
          log_buffer_ = new LogBuffer();
        }*/
            for (auto buf : log_buffers_){
                PersistLogBuffer(buf);
                delete buf;
            }
            log_buffers_.clear();

   }
     std::this_thread::sleep_for(
        std::chrono::microseconds(500000));

 }

}
}}
