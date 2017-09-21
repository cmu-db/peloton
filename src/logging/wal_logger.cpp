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
#include <boost/algorithm/string.hpp>
#include "gc/gc_manager_factory.h"
#include "concurrency/transaction_manager_factory.h"
#include "index/index_factory.h"

#include "catalog/catalog_defaults.h"
#include "catalog/column.h"
#include "catalog/manager.h"
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


void WalLogger::StartIndexRebulding() {
  PL_ASSERT(recovery_threads_.size() != 0);
  recovery_threads_[0].reset(new std::thread(&WalLogger::RunSecIndexRebuildThread, this));
}

void WalLogger::WaitForIndexRebuilding() {
  recovery_threads_[0]->join();
}

void WalLogger::StartRecovery(){

  GetSortedLogFileIdList();
  recovery_pools_.resize(1);
  recovery_threads_.resize(1);

  for (size_t i = 0; i < 1; ++i) {

    recovery_pools_[i].reset(new type::EphemeralPool());

    recovery_threads_[i].reset(new std::thread(&WalLogger::RunRecoveryThread, this));
  }
}

void WalLogger::WaitForRecovery() {
  for (auto &recovery_thread : recovery_threads_) {
    recovery_thread->join();
  }
}


void WalLogger::GetSortedLogFileIdList(){
  // Open the log dir
  struct dirent *file;
  DIR *dirp;
  dirp = opendir(this->log_dir_.c_str());
  if (dirp == nullptr) {
    LOG_ERROR("Can not open log directory %s\n", this->log_dir_.c_str());
    exit(EXIT_FAILURE);
  }


  // Filter out all log files
  std::string base_name = logging_filename_prefix_ + "_" + std::to_string(logger_id_) + "_";

  file_eids_.clear();

  while ((file = readdir(dirp)) != nullptr) {
    if (strncmp(file->d_name, base_name.c_str(), base_name.length()) == 0) {
      // Find one log file
      LOG_TRACE("Logger %d find a log file %s\n", (int) logger_id_, file->d_name);
      // Get the file epoch id
      size_t file_eid = (size_t)std::stoi(std::string(file->d_name + base_name.length()));

        file_eids_.push_back(file_eid);
    }
  }

  // Sort in descending order
  std::sort(file_eids_.begin(), file_eids_.end(), std::less<size_t>());
  max_replay_file_id_ = file_eids_.size() - 1;

}

txn_id_t WalLogger::LockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset) {
  txn_id_t txnid_logger = (this->logger_id_);
  while (true) {
    // We use the txn_id field as a lock. However this field also stored information about whether a tuple is deleted or not.
    // To restore that information, we need to return the old one before overwriting it.
    if (tg_header->SetAtomicTransactionId(tuple_offset, INITIAL_TXN_ID, txnid_logger) == INITIAL_TXN_ID) return INITIAL_TXN_ID;
    if (tg_header->SetAtomicTransactionId(tuple_offset, INVALID_TXN_ID, txnid_logger) == INVALID_TXN_ID) return INVALID_TXN_ID;
    _mm_pause();
  }
}

void WalLogger::UnlockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset, txn_id_t new_txn_id) {
  PL_ASSERT(new_txn_id == INVALID_TXN_ID || new_txn_id == INITIAL_TXN_ID);
  tg_header->SetAtomicTransactionId(tuple_offset, (txn_id_t) (this->logger_id_), new_txn_id);

}

bool WalLogger::InstallTupleRecord(LogRecordType type, storage::Tuple *tuple, storage::DataTable *table, cid_t cur_cid, ItemPointer location) {

    oid_t tile_group_id = location.block;
    auto tile_group_header = catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();

      auto tuple_slot = location.offset;


     if (type == LogRecordType::TUPLE_UPDATE) {
         ItemPointer insert_location = table->InsertTuple(tuple,nullptr);
         if (insert_location.block == INVALID_OID) {
           LOG_ERROR("Failed to get tuple slot");
           return false;
         }
         auto cid = tile_group_header->GetEndCommitId(tuple_slot);
         PL_ASSERT(cid > cur_cid);
         auto new_tile_group_header =
             catalog::Manager::GetInstance().GetTileGroup(insert_location.block)->GetHeader();
         new_tile_group_header->SetBeginCommitId(insert_location.offset,
                                                 cur_cid);
         new_tile_group_header->SetEndCommitId(insert_location.offset, cid);


         tile_group_header->SetEndCommitId(tuple_slot, cur_cid);

         // we should set the version before releasing the lock.

         new_tile_group_header->SetTransactionId(insert_location.offset,
                                                 INITIAL_TXN_ID);
         tile_group_header->SetTransactionId(tuple_slot, INVALID_TXN_ID);



      } else if (type == LogRecordType::TUPLE_DELETE) {
         ItemPointer insert_location = table->InsertEmptyVersion(); // This function does insert indexes
         if (insert_location.block == INVALID_OID) {
           LOG_ERROR("Failed to get tuple slot");
           return false;
         }

        auto cid = tile_group_header->GetEndCommitId(tuple_slot);
        PL_ASSERT(cid > cur_cid);
        auto new_tile_group_header =
            catalog::Manager::GetInstance().GetTileGroup(insert_location.block)->GetHeader();
        new_tile_group_header->SetBeginCommitId(insert_location.offset,
                                                cur_cid);
        new_tile_group_header->SetEndCommitId(insert_location.offset, cid);


        tile_group_header->SetEndCommitId(tuple_slot, cur_cid);

        // we should set the version before releasing the lock.
        new_tile_group_header->SetTransactionId(insert_location.offset,
                                                INVALID_TXN_ID);
        tile_group_header->SetTransactionId(tuple_slot, INITIAL_TXN_ID);

      } else if (type == LogRecordType::TUPLE_INSERT) {
        ItemPointer insert_location = table->InsertTuple(tuple,nullptr); // This function does insert indexes
        // set the begin commit id to persist insert
        tile_group_header->SetBeginCommitId(insert_location.offset, cur_cid);
        tile_group_header->SetEndCommitId(insert_location.offset, MAX_CID);
        tile_group_header->SetTransactionId(insert_location.offset, INITIAL_TXN_ID);
        tile_group_header->SetNextItemPointer(insert_location.offset, INVALID_ITEMPOINTER);

      }
      return true;
}

bool WalLogger::ReplayLogFile(FileHandle &file_handle){
  PL_ASSERT(file_handle.file != nullptr && file_handle.fd != INVALID_FILE_DESCRIPTOR);

  // Status
  cid_t current_cid = INVALID_CID;
  cid_t current_eid = INVALID_EID;
  size_t buf_size = 4096;
  std::unique_ptr<char[]> buffer(new char[buf_size]);
  char length_buf[sizeof(int32_t)];
  std::vector<catalog::Column> columns;
  std::vector<index::Index> indexes;
  while (true) {
    // Read the frame length
    if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) &length_buf, 4) == false) {
      LOG_TRACE("Reach the end of the log file");
      break;
    }
    CopySerializeInput length_decode((const void *) &length_buf, 4);
    int length = length_decode.ReadInt();
    // Adjust the buffer
    if ((size_t) length > buf_size) {
      buffer.reset(new char[(int)(length * 1.2)]);
      buf_size = (size_t) length;
    }

    if (LoggingUtil::ReadNBytesFromFile(file_handle, (void *) buffer.get(), length) == false) {
      LOG_ERROR("Unexpected file eof");
      // TODO: How to handle damaged log file?
      return false;
    }
    CopySerializeInput record_decode((const void *) buffer.get(), length);


    /*
     * Decode the record
     */
    // Get record type
    LogRecordType record_type = (LogRecordType) (record_decode.ReadEnumInSingleByte());
    switch (record_type) {
    case LogRecordType::TRANSACTION_BEGIN: {
        current_cid = (cid_t) record_decode.ReadLong();

        break;
      } case LogRecordType::TRANSACTION_COMMIT: {
        cid_t cid = (cid_t) record_decode.ReadLong();
        if (cid != current_cid) {
          LOG_ERROR("Mismatched txn in log record");
          return false;
        }
        current_cid = INVALID_CID;
        break;
      }
      case LogRecordType::TUPLE_INSERT:
        {
          current_eid = record_decode.ReadLong();
          current_cid = record_decode.ReadLong();

          oid_t database_id = (oid_t) record_decode.ReadLong();
          oid_t table_id = (oid_t) record_decode.ReadLong();

          oid_t tg_block = (oid_t) record_decode.ReadLong();
          oid_t tg_offset = (oid_t) record_decode.ReadLong();

          ItemPointer location(tg_block, tg_offset);
          auto table = storage::StorageManager::GetInstance()->GetTableWithOid(database_id, table_id);
          auto schema = table->GetSchema();
          auto tg = table->GetTileGroupById(tg_block);
          //The required tile group might not have been created yet
          if(tg.get() == nullptr)
          {
            table->AddTileGroupWithOidForRecovery(tg_block);
            tg = table->GetTileGroupById(tg_block);
          }

          std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema,true));
              for(oid_t oid = 0; oid < schema->GetColumns().size(); oid++){
                  type::Value val = type::Value::DeserializeFrom(record_decode, schema->GetColumn(oid).GetType());
                  tuple->SetValue(oid, val);
              }
              //Catalog is already created, we must do this transactionally
          if(database_id == CATALOG_DATABASE_OID){ //catalog database oid
              InstallTupleRecord(record_type, tuple.get(), table, current_cid, location);
              switch (table_id){
                  case TABLE_CATALOG_OID: //pg_table
                      {
                      auto database = storage::StorageManager::GetInstance()->GetDatabaseWithOid(tuple->GetValue(2).GetAs<oid_t>()); //Getting database oid from pg_table
                      database->AddTable(new storage::DataTable(new catalog::Schema(columns),tuple->GetValue(1).ToString(),database->GetOid(),tuple->GetValue(0).GetAs<oid_t>(),1000,true,false,false));
                      LOG_DEBUG("\n\n\nPG_TABLE\n\n\n");
                      columns.clear();
                      break;
              }
                  case COLUMN_CATALOG_OID: //pg_attribute
                      {
                      std::string typeId = tuple->GetValue(4).ToString();
                      type::TypeId column_type = StringToTypeId(typeId);
                      uint index = stoi(tuple->GetValue(2).ToString());
                      if(index >= columns.size())
                      {
                          //Made to fit index as the last element
                          columns.resize(index+1);
                      }
                      if(column_type == type::TypeId::VARCHAR || column_type == type::TypeId::VARBINARY){
                          catalog::Column tmp_col(column_type,type::Type::GetTypeSize(column_type),tuple->GetValue(1).ToString(),false,tuple->GetValue(1).GetAs<oid_t>());
                          columns[index] = tmp_col;
                      } else {
                          catalog::Column tmp_col(column_type,type::Type::GetTypeSize(column_type),tuple->GetValue(1).ToString(),true,tuple->GetValue(1).GetAs<oid_t>());
                          columns[index] = tmp_col;
                        //  columns.insert(columns.begin(), catalog::Column();
                      }
                      LOG_DEBUG("\n\n\nPG_ATTRIBUTE\n\n\n");
                      break;
              }
              case INDEX_CATALOG_OID:
              {
                //Attributes must be changed once we have arraytype
//                std::vector<std::string> attrs;
//                boost::split(attrs, tuple->GetValue(6).ToString, boost::is_any_of(" "));
              }
              }
          } else {
              //Simply insert the tuple in the tilegroup directly
              tg->InsertTupleFromRecovery(current_cid, tg_offset, tuple.get());
          }
          break;
    }
      case LogRecordType::TUPLE_DELETE:
    {
        current_eid = record_decode.ReadLong();
        current_cid = record_decode.ReadLong();
          oid_t database_id = (oid_t) record_decode.ReadLong();
          oid_t table_id = (oid_t) record_decode.ReadLong();

          oid_t tg_block = (oid_t) record_decode.ReadLong();
          oid_t tg_offset = (oid_t) record_decode.ReadLong();

          auto table = storage::StorageManager::GetInstance()->GetTableWithOid(database_id, table_id);
          auto tg = table->GetTileGroupById(tg_block);
        //This code might be useful on drop
        if(database_id == CATALOG_DATABASE_OID){ //catalog database oid
              switch (table_id){
                  case TABLE_CATALOG_OID: //pg_table
                      {
                      auto db_oid = tg->GetValue(tg_offset, 2).GetAs<oid_t>();
                      auto table_oid = tg->GetValue(tg_offset, 0).GetAs<oid_t>();
                      auto database = storage::StorageManager::GetInstance()->GetDatabaseWithOid(db_oid); //Getting database oid from pg_table
                      database->DropTableWithOid(table_oid);
                      LOG_DEBUG("\n\n\nPG_TABLE\n\n\n");
                      break;
              }

              }
          }

            tg->DeleteTupleFromRecovery(current_cid, tg_offset);
          break;
    }
      case LogRecordType::TUPLE_UPDATE:{
        current_eid = record_decode.ReadLong();
        current_cid = record_decode.ReadLong();
        oid_t database_id = (oid_t) record_decode.ReadLong();
        oid_t table_id = (oid_t) record_decode.ReadLong();
        oid_t old_tg_block = (oid_t) record_decode.ReadLong();
        oid_t old_tg_offset = (oid_t) record_decode.ReadLong();
        oid_t tg_block = (oid_t) record_decode.ReadLong();
        oid_t tg_offset = (oid_t) record_decode.ReadLong();

        ItemPointer location(tg_block, tg_offset);
        auto table = storage::StorageManager::GetInstance()->GetTableWithOid(database_id, table_id);
        auto old_tg = table->GetTileGroupById(old_tg_block);
        auto tg = table->GetTileGroupById(tg_block);
        //The required tile group might not have been created yet
        if(tg.get() == nullptr)
        {
          table->AddTileGroupWithOidForRecovery(tg_block);
          tg = table->GetTileGroupById(tg_block);
        }

            // XXX: We still rely on an alive catalog manager
            auto schema = table->GetSchema();

            // Decode the tuple from the record
            std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema,true));
           


            for(oid_t oid = 0; oid < schema->GetColumns().size(); oid++){
                type::Value val = type::Value::DeserializeFrom(record_decode, schema->GetColumn(oid).GetType());
                tuple->SetValue(oid, val);
            }
        tg->InsertTupleFromRecovery(current_cid,tg_offset,tuple.get());
        old_tg->UpdateTupleFromRecovery(current_cid, old_tg_offset, location);
        /*
        if(database_id == 16777216){ //catalog database oid
            switch (table_id){
                case 33554433: //pg_table
                    {
                    auto database = storage::StorageManager::GetInstance()->GetDatabaseWithOid(tuple->GetValue(2).GetAs<oid_t>()); //Getting database oid from pg_table
                    database->AddTable(new storage::DataTable(new catalog::Schema(columns),tuple->GetValue(1).ToString(),database->GetOid(),tuple->GetValue(0).GetAs<oid_t>(),1000,true,false,false));
                    LOG_DEBUG("\n\n\nPG_TABLE\n\n\n");
                    columns.clear();
                    break;}
                case 33554435: //pg_attribute
                    {
                    std::string typeId = tuple->GetValue(4).ToString();
                    type::TypeId column_type = StringToTypeId(typeId);
                    if(column_type == type::TypeId::VARCHAR || column_type == type::TypeId::VARBINARY){
                        columns.insert(columns.begin(), catalog::Column(column_type,type::Type::GetTypeSize(column_type),tuple->GetValue(1).ToString(),false,tuple->GetValue(1).GetAs<oid_t>()));
                    } else {
                        columns.insert(columns.begin(),catalog::Column(column_type,type::Type::GetTypeSize(column_type),tuple->GetValue(1).ToString(),true,tuple->GetValue(1).GetAs<oid_t>()));
                    }
                    LOG_DEBUG("\n\n\nPG_ATTRIBUTE\n\n\n");
                    break;}
            }
        }*/
        break;
      }

      default:
      LOG_ERROR("Unknown log record type");
        return false;
    }

  }
  if(current_eid != INVALID_EID){
      auto& epoch_manager = concurrency::EpochManagerFactory::GetInstance();
      epoch_manager.Reset(current_eid);
      epoch_manager.StartEpoch();
  }
  return true;
}



void WalLogger::RunRecoveryThread(){

    int replay_cap = max_replay_file_id_.load();
  while (true) {

    int replay_file_id = max_replay_file_id_.fetch_sub(1, std::memory_order_relaxed);
    if (replay_file_id < 0) {
      break;
    }

    size_t file_eid = file_eids_.at(replay_cap - replay_file_id);
    // Replay a single file
    std::string filename = GetLogFileFullPath(file_eid);
    FileHandle file_handle;
    bool res = LoggingUtil::OpenFile(filename.c_str(), "rb", file_handle);
    if (res == false) {
      LOG_ERROR("Cannot open log file %s\n", filename.c_str());
      exit(EXIT_FAILURE);
    }
    ReplayLogFile(file_handle);

    // Safely close the file
    res = LoggingUtil::CloseFile(file_handle);
    if (res == false) {
      LOG_ERROR("Cannot close pepoch file");
      exit(EXIT_FAILURE);
    }

  }

}

void WalLogger::RunSecIndexRebuildThread() {
  auto manager = storage::StorageManager::GetInstance();
  auto db_count = manager->GetDatabaseCount();

  // Loop all databases
  for (oid_t db_idx = 0; db_idx < db_count; db_idx ++) {
    auto database = manager->GetDatabaseWithOid(db_idx);
    auto table_count = database->GetTableCount();

    // Loop all tables
    for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
      // Get the target table
      storage::DataTable *table = database->GetTable(table_idx);
      RebuildSecIndexForTable(table);
    }
  }
}

void WalLogger::RebuildSecIndexForTable(storage::DataTable *table) {
  size_t tg_count = table->GetTileGroupCount();

  // Loop all the tile groups, shared by thread id
  for (size_t tg_idx = 0; tg_idx < tg_count; tg_idx ++) {
    auto tg = table->GetTileGroup(tg_idx).get();
    UNUSED_ATTRIBUTE auto tg_header = tg->GetHeader();
    oid_t active_tuple_count = tg->GetNextTupleSlot();

    // Loop all tuples headers in the tile group
    for (oid_t tuple_offset = 0; tuple_offset < active_tuple_count; ++tuple_offset) {
      // Check if the tuple is valid
      PL_ASSERT(tg_header->GetTransactionId(tuple_offset) == INITIAL_TXN_ID);
    }
  }
}


std::unique_ptr<LogBuffer> WalLogger::PersistLogBuffer(std::unique_ptr<LogBuffer> log_buffer) {
    FileHandle *new_file_handle = new FileHandle();
    if(likely_branch(log_buffer != nullptr)){
    std::string filename = GetLogFileFullPath(0);
    // Create a new file
    if (LoggingUtil::OpenFile(filename.c_str(), "ab", *new_file_handle) == false) {
      LOG_ERROR("Unable to create log file %s\n", filename.c_str());
      exit(EXIT_FAILURE);
    }

    fwrite((const void *) (log_buffer->GetData()), log_buffer->GetSize(), 1, new_file_handle->file);
    log_buffer->Reset();

//  Call fsync
    LoggingUtil::FFlushFsync(*new_file_handle);

    bool res = LoggingUtil::CloseFile(*new_file_handle);
    if (res == false) {
      LOG_ERROR("Cannot close log file under directory %s", log_dir_.c_str());
      exit(EXIT_FAILURE);
    }
}
    delete new_file_handle;
return std::move(log_buffer);
}


}
}
