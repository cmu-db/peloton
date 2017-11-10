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

#include "logging/wal_recovery.h"

namespace peloton {
namespace logging {



void WalRecovery::StartRecovery(){

  GetSortedLogFileIdList();
  RunRecovery();
}



void WalRecovery::GetSortedLogFileIdList(){
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
  closedir(dirp);

}

txn_id_t WalRecovery::LockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset) {
  txn_id_t txnid_logger = (this->logger_id_);
  while (true) {
    // We use the txn_id field as a lock. However this field also stored information about whether a tuple is deleted or not.
    // To restore that information, we need to return the old one before overwriting it.
    if (tg_header->SetAtomicTransactionId(tuple_offset, INITIAL_TXN_ID, txnid_logger) == INITIAL_TXN_ID) return INITIAL_TXN_ID;
    if (tg_header->SetAtomicTransactionId(tuple_offset, INVALID_TXN_ID, txnid_logger) == INVALID_TXN_ID) return INVALID_TXN_ID;
    _mm_pause();
  }
}

void WalRecovery::UnlockTuple(storage::TileGroupHeader *tg_header, oid_t tuple_offset, txn_id_t new_txn_id) {
  PL_ASSERT(new_txn_id == INVALID_TXN_ID || new_txn_id == INITIAL_TXN_ID);
  tg_header->SetAtomicTransactionId(tuple_offset, (txn_id_t) (this->logger_id_), new_txn_id);

}

bool WalRecovery::InstallTupleRecord(LogRecordType type, storage::Tuple *tuple, storage::DataTable *table, cid_t cur_cid, ItemPointer location) {

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

bool WalRecovery::ReplayLogFile(FileHandle &file_handle){
  PL_ASSERT(file_handle.file != nullptr && file_handle.fd != INVALID_FILE_DESCRIPTOR);

  // Status
  cid_t current_cid = INVALID_CID;
  cid_t current_eid = INVALID_EID;
  size_t buf_size = 4096;
  std::unique_ptr<char[]> buffer(new char[buf_size]);
  char length_buf[sizeof(int32_t)];
  std::vector<catalog::Column> columns;
  //Store the pg_index tuples to defer index creation
  std::vector<std::unique_ptr<storage::Tuple>> indexes;
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
          eid_t record_eid = record_decode.ReadLong();
          if(record_eid > current_eid){
            current_eid =  record_eid;
          }
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
            catalog::Manager::GetInstance().GetNextTileGroupId();
          }

          std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema,true));
              for(oid_t oid = 0; oid < schema->GetColumns().size(); oid++){
                  type::Value val = type::Value::DeserializeFrom(record_decode, schema->GetColumn(oid).GetType());
                  tuple->SetValue(oid, val);
              }
             //When inserts happen in catalog tables, there must be a corresponding data structure
          if(database_id == CATALOG_DATABASE_OID){ //catalog database oid
               //Catalog is already created, we must do this transactionally
              InstallTupleRecord(record_type, tuple.get(), table, current_cid, location);
              switch (table_id){
                  case TABLE_CATALOG_OID: //pg_table
                      {
                      auto database = storage::StorageManager::GetInstance()->GetDatabaseWithOid(tuple->GetValue(2).GetAs<oid_t>()); //Getting database oid from pg_table
                      database->AddTable(new storage::DataTable(new catalog::Schema(columns),tuple->GetValue(1).ToString(),database->GetOid(),tuple->GetValue(0).GetAs<oid_t>(),DEFAULT_TUPLES_PER_TILEGROUP,true,false,false));
                      LOG_DEBUG("\n\n\nPG_TABLE\n\n\n");
                      catalog::TableCatalog::GetInstance()->GetNextOid();
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
                      catalog::ColumnCatalog::GetInstance()->GetNextOid();
                      LOG_DEBUG("\n\n\nPG_ATTRIBUTE\n\n\n");
                      break;
              }
              case INDEX_CATALOG_OID:
              {


                  LOG_DEBUG("\n\n\nPG_INDEX\n\n\n");
                  indexes.push_back(std::move(tuple));
                  catalog::IndexCatalog::GetInstance()->GetNextOid();

              }
              }
          } else {
              //Simply insert the tuple in the tilegroup directly
              auto tuple_id = tg->InsertTupleFromRecovery(current_cid, tg_offset, tuple.get());
              table->IncreaseTupleCount(1);
              if (tuple_id == tg->GetAllocatedTupleCount() - 1) {
                if(table->GetTileGroupById(tg->GetTileGroupId()+1).get() == nullptr)
                    table->AddTileGroupWithOidForRecovery(tg->GetTileGroupId()+1);
                    catalog::Manager::GetInstance().GetNextTileGroupId();
              }
          }
          break;
    }
      case LogRecordType::TUPLE_DELETE:
    {
        eid_t record_eid = record_decode.ReadLong();
        if(record_eid > current_eid){
          current_eid =  record_eid;
        }
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
              case INDEX_CATALOG_OID: //pg_index
                  {
                  std::vector<std::unique_ptr<storage::Tuple>>::iterator pos =
                  std::find_if(indexes.begin(), indexes.end(), [&tg, &tg_offset](const std::unique_ptr<storage::Tuple> &arg) {
                                                             return arg->GetValue(0).CompareEquals(tg->GetValue(tg_offset, 0)); });
                  if(pos != indexes.end())
                      indexes.erase(pos);
                  }

              }
          }
        auto tuple_id =  tg->DeleteTupleFromRecovery(current_cid, tg_offset);
        table->IncreaseTupleCount(1);
        if (tuple_id == tg->GetAllocatedTupleCount() - 1) {
            if(table->GetTileGroupById(tg->GetTileGroupId()+1).get() == nullptr)
                table->AddTileGroupWithOidForRecovery(tg->GetTileGroupId()+1);
                catalog::Manager::GetInstance().GetNextTileGroupId();
        }
        break;
    }
      case LogRecordType::TUPLE_UPDATE:{
        eid_t record_eid = record_decode.ReadLong();
        if(record_eid > current_eid){
          current_eid =  record_eid;
        }
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
       auto tuple_id =  tg->InsertTupleFromRecovery(current_cid,tg_offset,tuple.get());
        old_tg->UpdateTupleFromRecovery(current_cid, old_tg_offset, location);
        table->IncreaseTupleCount(1);
        if (tuple_id == tg->GetAllocatedTupleCount() - 1) {
            if(table->GetTileGroupById(tg->GetTileGroupId()+1).get() == nullptr)
                table->AddTileGroupWithOidForRecovery(tg->GetTileGroupId()+1);
                catalog::Manager::GetInstance().GetNextTileGroupId();
        }
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
            auto txn = concurrency::TransactionManagerFactory::GetInstance().BeginTransaction(IsolationLevelType::SERIALIZABLE);
            ItemPointer* i = new ItemPointer(tg, offset);
            table->InsertInIndexes(t, p, txn, &i);
            concurrency::TransactionManagerFactory::GetInstance().CommitTransaction(txn);
          }

      }

  }
  return true;
}



void WalRecovery::RunRecovery(){

    //size_t file_eid = file_eids_.at(replay_cap - replay_file_id);
    // Replay a single file
    std::string filename = GetLogFileFullPath(0);
    FileHandle file_handle;
    bool res = LoggingUtil::OpenFile(filename.c_str(), "rb", file_handle);
    if (res == false) {
      LOG_ERROR("Cannot open log file %s\n", filename.c_str());
      //exit(EXIT_FAILURE);
    } else {
    ReplayLogFile(file_handle);

    // Safely close the file
    res = LoggingUtil::CloseFile(file_handle);
    if (res == false) {
      LOG_ERROR("Cannot close pepoch file");
      exit(EXIT_FAILURE);
    }
    }
  }



void WalRecovery::RunSecIndexRebuildThread() {

}

void WalRecovery::RebuildSecIndexForTable(storage::DataTable *table UNUSED_ATTRIBUTE) {

}

}}
