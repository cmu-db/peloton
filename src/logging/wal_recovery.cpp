
#include "index/index_factory.h"

#include "catalog/index_catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/database_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "catalog/catalog_defaults.h"
#include "type/ephemeral_pool.h"
#include "catalog/manager.h"
#include "concurrency/epoch_manager_factory.h"
#include "logging/wal_recovery.h"
#include "logging/log_util.h"
#include "common/logger.h"


namespace peloton{
namespace logging{

void WalRecovery::StartRecovery(){

  if(!LoggingUtil::OpenFile(log_path_.c_str(),
                            std::ifstream::in, fstream_)) {

    LOG_ERROR("Recovery failed");
    return;
  }

  ReplayLogFile();
}


void WalRecovery::ParseFromDisk(ReplayStage stage){

  int  buf_size = 4096, buf_rem = 0, buf_curr = 0,total_len=0;
  char *buf = new char[buf_size];
  bool replay_complete = false;

  fstream_.clear();
  fstream_.seekg(0, std::ios::beg);

  while(!replay_complete) {

    int record_len,  nbytes = buf_size-buf_rem;
    int bytes_read = LoggingUtil::ReadNBytesFromFile(fstream_, buf+buf_curr, nbytes);

    LOG_INFO("Read %d bytes from the log", bytes_read);

    buf_rem += bytes_read;

    if(bytes_read != nbytes){
      if(fstream_.eof()){
        replay_complete = true;

        if(buf_rem==0)
          continue;
      }
    }

    while(buf_rem >= (int)sizeof(record_len)){
      CopySerializeInput length_decode((const void *)(buf+buf_curr), 4);
      record_len = length_decode.ReadInt();

      if(buf_rem >= (int)(record_len + sizeof(record_len))){

        total_len += record_len;

        int record_offset = buf_curr + sizeof(record_len);

        /***********/
        /*  PASS 1 */
        /***********/

        if(stage==ReplayStage::PASS_1){
          CopySerializeInput record_decode((const void *)(buf+record_offset), record_len);
          LogRecordType record_type = (LogRecordType)(record_decode.ReadEnumInSingleByte());
          txn_id_t txn_id = record_decode.ReadLong();

          int req_mem_size = record_len + sizeof(record_len);

          switch(record_type){
            case LogRecordType::TRANSACTION_BEGIN:{
              if(all_txns_.find(txn_id) != all_txns_.end()){
                LOG_ERROR("Duplicate transaction");
                PELOTON_ASSERT(false);
              }

              auto value = std::make_pair(record_type, req_mem_size);
              all_txns_.insert(std::make_pair(txn_id, value));

              break;
            }

            case LogRecordType::TRANSACTION_COMMIT:{
              // keeps track of the memory that needs to be allocated.
              log_buffer_size_ += (req_mem_size+all_txns_[txn_id].second);
            }

            // intentional fall through
            case LogRecordType::TRANSACTION_ABORT:
            case LogRecordType::TUPLE_DELETE:
            case LogRecordType::TUPLE_UPDATE:
            case LogRecordType::TUPLE_INSERT: {

              if(all_txns_.find(txn_id) == all_txns_.end()){
                LOG_ERROR("Transaction not found");
                PELOTON_ASSERT(false);
              }

              all_txns_[txn_id].first = record_type;
              all_txns_[txn_id].second += req_mem_size;

              break;
            }

            default: {
              LOG_ERROR("Unknown LogRecordType.");
              PELOTON_ASSERT(false);
            }
          }
        }

        else if (stage==ReplayStage::PASS_2) {

          CopySerializeInput record_decode((const void *)(buf+record_offset), record_len);
          LogRecordType record_type = (LogRecordType)(record_decode.ReadEnumInSingleByte());
          txn_id_t txn_id = record_decode.ReadLong();

          (void) record_type;

          if(commited_txns_.find(txn_id)!=commited_txns_.end()) {
            int copy_len = record_len + sizeof(record_len);
            int curr_offset = commited_txns_[txn_id].second;

            LOG_INFO("txn %llu writing from %d to %d", txn_id, curr_offset, curr_offset+copy_len-1);

            PELOTON_MEMCPY(log_buffer_+curr_offset, buf+buf_curr, copy_len);
            commited_txns_[txn_id].second += copy_len;
          }
        }

        buf_curr += (record_len + sizeof(record_len));
        buf_rem  -= (record_len + sizeof(record_len));
      } else {
        break;
      }
    }

    PELOTON_MEMCPY(buf, buf+buf_curr, buf_rem);
    PELOTON_MEMSET(buf+buf_rem, 0, buf_size - buf_rem);
    buf_curr = buf_rem;
  }

  delete[] buf;
}



bool WalRecovery::InstallCatalogTuple(LogRecordType record_type, storage::Tuple *tuple,
                               storage::DataTable *table, cid_t cur_cid,
                               ItemPointer location){

  ItemPointer *i = nullptr;

  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction(IsolationLevelType::SERIALIZABLE);

  oid_t tile_group_id = location.block;
  auto tile_group = table->GetTileGroupById(location.block);
  auto tile_group_header =
          catalog::Manager::GetInstance().GetTileGroup(tile_group_id)->GetHeader();

  if(record_type==LogRecordType::TUPLE_INSERT){

    PELOTON_ASSERT(tile_group != nullptr);

    auto status = tile_group_header->GetEmptyTupleSlot(location.offset);

    (void) status;

    tile_group->CopyTuple(tuple, location.offset);
    bool result = table->InsertTuple(tuple, location, txn, &i);

    (void) result;

    tile_group_header->SetBeginCommitId(location.offset, cur_cid);
    tile_group_header->SetEndCommitId(location.offset, MAX_CID);
    tile_group_header->SetTransactionId(location.offset, INITIAL_TXN_ID);
    tile_group_header->SetNextItemPointer(location.offset,
                                          INVALID_ITEMPOINTER);
    tile_group_header->SetIndirection(location.offset,i);
  }

  txn_manager.CommitTransaction(txn);
  return true;
}

void WalRecovery::CreateTableOnRecovery(std::unique_ptr<storage::Tuple> &tuple,
                                        std::vector<catalog::Column> &columns){

  // Getting database
  auto database =
          storage::StorageManager::GetInstance()->GetDatabaseWithOid(
                  tuple->GetValue(2).GetAs<oid_t>());

  // register table with catalog
  database->AddTable(new storage::DataTable(
          new catalog::Schema(columns), tuple->GetValue(1).ToString(),
          database->GetOid(), tuple->GetValue(0).GetAs<oid_t>(),
          DEFAULT_TUPLES_PER_TILEGROUP, true, false, false));

  columns.clear();
}

void WalRecovery::ReplaySingleTxn(txn_id_t txn_id){
  int start_offset = commited_txns_[txn_id].first;
  int curr_offset  = start_offset;
  int total_len = all_txns_[txn_id].second;
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());

  std::vector<std::unique_ptr<storage::Tuple>> indexes;


  bool pending_table_create;
  std::unique_ptr<storage::Tuple> tuple_table_create;
  std::vector<catalog::Column> columns;

  while(total_len > 0){
    CopySerializeInput length_decode((const void *)(log_buffer_+curr_offset), sizeof(int));
    int record_len = length_decode.ReadInt();

    curr_offset += sizeof(int);

    CopySerializeInput record_decode((const void *)(log_buffer_+curr_offset), record_len);

    LogRecordType record_type = (LogRecordType)(record_decode.ReadEnumInSingleByte());
    txn_id_t txn_id = record_decode.ReadLong();
    eid_t  epoch_id = record_decode.ReadLong();
    cid_t commit_id = record_decode.ReadLong();

    (void) txn_id;

    if(max_epoch_id_==INVALID_EID)
      max_epoch_id_ = epoch_id;
    else if(epoch_id>max_epoch_id_)
      max_epoch_id_ = epoch_id;

    // complete table_creation if pending
    if(record_type != LogRecordType::TUPLE_INSERT){
      if(pending_table_create) {
        CreateTableOnRecovery(tuple_table_create, columns);
        pending_table_create = false;
      }
    }

    if(record_type==LogRecordType::TUPLE_INSERT) {

      oid_t database_id = (oid_t) record_decode.ReadLong();
      oid_t table_id = (oid_t) record_decode.ReadLong();

      if(database_id != CATALOG_DATABASE_OID || table_id != COLUMN_CATALOG_OID){
        if(pending_table_create){
          CreateTableOnRecovery(tuple_table_create, columns);
          pending_table_create = false;
        }
      }

      oid_t tg_block = (oid_t) record_decode.ReadLong();
      oid_t tg_offset = (oid_t) record_decode.ReadLong();

      ItemPointer location(tg_block, tg_offset);
      auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
              database_id, table_id);
      auto schema = table->GetSchema();
      auto tg = table->GetTileGroupById(tg_block);

      // Create the tile group if it does not exist
      if (tg.get() == nullptr) {
        table->AddTileGroupWithOidForRecovery(tg_block);
        tg = table->GetTileGroupById(tg_block);
        catalog::Manager::GetInstance().GetNextTileGroupId();
      }

      std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
      for (oid_t oid = 0; oid < schema->GetColumns().size(); oid++) {
        type::Value val = type::Value::DeserializeFrom(
                record_decode, schema->GetColumn(oid).GetType());
        tuple->SetValue(oid, val, pool.get());
      }

      if (database_id == CATALOG_DATABASE_OID) {  // catalog database oid


        if (table_id == DATABASE_CATALOG_OID) {

          LOG_INFO("REPLAYING INSERT TO PG_DATABASE");

          // insert tuple into pg_database
          InstallCatalogTuple(record_type, tuple.get(), table, commit_id, location);

          auto storage_manager = storage::StorageManager::GetInstance();
          auto pg_database  = catalog::DatabaseCatalog::GetInstance();

          auto database_oid = tuple->GetValue(0).GetAs<oid_t>();
          auto database_name = tuple->GetValue(1).ToString();

          storage::Database *database = new storage::Database(database_oid);
          database->setDBName(database_name);

          // register database with storage manager
          storage_manager->AddDatabaseToStorageManager(database);

          // update database oid to prevent oid reuse after recovery
          pg_database->GetNextOid();  // TODO(graghura): This is a hack, rewrite !!!

        } else if (table_id == TABLE_CATALOG_OID) {

          LOG_INFO("REPLAYING INSERT TO PG_TABLE");

          // insert table info into pg_table
          InstallCatalogTuple(record_type, tuple.get(), table, commit_id, location);

          // update table oid to prevent oid reuse after recovery
          catalog::TableCatalog::GetInstance()->GetNextOid();  // TODO(graghura): This is a hack, rewrite !!!
          tuple_table_create = std::move(tuple);
          pending_table_create = true;

        } else if (table_id == COLUMN_CATALOG_OID) {

          LOG_INFO("REPLAYING INSERT TO PG_COLUMN");

          // insert tuple into pg_column - one for every attribute in the schema
          InstallCatalogTuple(record_type, tuple.get(), table, commit_id, location);

          // update column oid to prevent oid reuse after recovery
          catalog::ColumnCatalog::GetInstance()->GetNextOid();  // TODO(graghura): This is a hack, rewrite !!!

          bool is_inline = true;

          std::string typeId = tuple->GetValue(4).ToString();
          type::TypeId column_type = StringToTypeId(typeId);

          if (column_type == type::TypeId::VARCHAR ||
              column_type == type::TypeId::VARBINARY) {
              is_inline = false;
          }

          catalog::Column tmp_col(column_type,
                                  type::Type::GetTypeSize(column_type),
                                  tuple->GetValue(1).ToString(), is_inline,
                                  tuple->GetValue(1).GetAs<oid_t>());

          uint index = stoi(tuple->GetValue(2).ToString());

          if (index >= columns.size()) {
            columns.resize(index + 1);
          }
          columns[index] = tmp_col;

        } else if (table_id == INDEX_CATALOG_OID) {
          LOG_INFO("REPLAYING INSERT TO PG_INDEX");
          indexes.push_back(std::move(tuple));
          catalog::IndexCatalog::GetInstance()->GetNextOid();  // TODO(graghura): This is a hack, rewrite !!!
        }
      } else {

        LOG_INFO("REPLAYING INSERT TO NON CATALOG TABLE");

        // insert tuple
        auto tuple_id = tg->InsertTupleFromRecovery(commit_id, tg_offset, tuple.get());
        table->IncreaseTupleCount(1);
        if (tuple_id == tg->GetAllocatedTupleCount() - 1) {
          // add a new tile group if the last slot has been taken
          if (table->GetTileGroupById(tg->GetTileGroupId() + 1).get() ==
              nullptr)
            table->AddTileGroupWithOidForRecovery(tg->GetTileGroupId() + 1);
          catalog::Manager::GetInstance().GetNextTileGroupId();
        }
      }
    }

    else if(record_type==LogRecordType::TUPLE_DELETE) {
      LOG_INFO("Tuple Delete");

      oid_t database_id = (oid_t)record_decode.ReadLong();
      oid_t table_id = (oid_t)record_decode.ReadLong();

      oid_t tg_block = (oid_t)record_decode.ReadLong();
      oid_t tg_offset = (oid_t)record_decode.ReadLong();

      auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
              database_id, table_id);
      auto tg = table->GetTileGroupById(tg_block);

      // This code might be useful on drop
      if (database_id == CATALOG_DATABASE_OID) {  // catalog database oid
        switch (table_id) {

          case DATABASE_CATALOG_OID:{ //pg_database
            // TODO(graghura): delete database
          }

          case TABLE_CATALOG_OID: {  // pg_table
            auto db_oid = tg->GetValue(tg_offset, 2).GetAs<oid_t>();
            auto table_oid = tg->GetValue(tg_offset, 0).GetAs<oid_t>();
            auto database =
                    storage::StorageManager::GetInstance()->GetDatabaseWithOid(
                            db_oid);  // Getting database oid from pg_table
            database->DropTableWithOid(table_oid);
            break;
          }

          case INDEX_CATALOG_OID: { // pg_index
            // TODO(graghura): delete index
          }

          case COLUMN_CATALOG_OID: { // pg_column
            // TODO(graghura): delete column (I think schema updates are not supported now)
          }

        }
      }

      auto tuple_id = tg->DeleteTupleFromRecovery(commit_id, tg_offset);
      table->IncreaseTupleCount(1);
      if (tuple_id == tg->GetAllocatedTupleCount() - 1) {
        if (table->GetTileGroupById(tg->GetTileGroupId() + 1).get() ==
            nullptr)
          table->AddTileGroupWithOidForRecovery(tg->GetTileGroupId() + 1);
        catalog::Manager::GetInstance().GetNextTileGroupId();
      }
      break;
    }

    else if(record_type==LogRecordType::TUPLE_UPDATE) {
      LOG_INFO("Replaying TUPLE_UPDATE");
    }

    else if(record_type==LogRecordType::TRANSACTION_BEGIN) {
      LOG_INFO("Replaying TXN_BEGIN");
    }

    else if(record_type==LogRecordType::TRANSACTION_COMMIT) {
      LOG_INFO("Replaying TXN_COMMIT");
    }

    else if(record_type==LogRecordType::TRANSACTION_ABORT) {
      LOG_ERROR("Shouldn't be replaying TXN_ABORT");
      PELOTON_ASSERT(false);
    }

    curr_offset += record_len;
    total_len -= record_len + sizeof(int);
  }

  if(pending_table_create) {
    CreateTableOnRecovery(tuple_table_create, columns);
  }


  std::set<storage::DataTable *> tables_with_indexes;

  // Index construction that was deferred from the read records phase
  for (auto const &tup : indexes) {
    LOG_INFO("Install Index");
    std::vector<oid_t> key_attrs;
    oid_t key_attr;
    auto txn = concurrency::TransactionManagerFactory::GetInstance().BeginTransaction(
                    IsolationLevelType::SERIALIZABLE);

    auto table_catalog = catalog::TableCatalog::GetInstance();
    auto table_object = table_catalog->GetTableObject(tup->GetValue(2).GetAs<oid_t>(), txn);
    auto database_oid = table_object->GetDatabaseOid();
    auto table = storage::StorageManager::GetInstance()->GetTableWithOid(
            database_oid, table_object->GetTableOid());
    concurrency::TransactionManagerFactory::GetInstance().CommitTransaction(
            txn);
    auto tuple_schema = table->GetSchema();
    std::stringstream iss(tup->GetValue(6).ToString());
    while (iss >> key_attr) key_attrs.push_back(key_attr);
    auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    index::IndexMetadata *index_metadata = new index::IndexMetadata(
            tup->GetValue(1).ToString(), tup->GetValue(0).GetAs<oid_t>(),
            table->GetOid(), database_oid,
            static_cast<IndexType>(tup->GetValue(3).GetAs<oid_t>()),
            static_cast<IndexConstraintType>(tup->GetValue(4).GetAs<oid_t>()),
            tuple_schema, key_schema, key_attrs, tup->GetValue(4).GetAs<bool>());
    std::shared_ptr<index::Index> index(
            index::IndexFactory::GetIndex(index_metadata));
    table->AddIndex(index);
    tables_with_indexes.insert(table);
    // Attributes must be changed once we have arraytype
  }

  // add tuples to index
  for (storage::DataTable *table : tables_with_indexes) {
    LOG_INFO("Install table_index");
    auto schema = table->GetSchema();
    size_t tg_count = table->GetTileGroupCount();
    for (oid_t tg = 0; tg < tg_count; tg++) {
      auto tile_group = table->GetTileGroup(tg);

      for (int tuple_slot_id = START_OID; tuple_slot_id < DEFAULT_TUPLES_PER_TILEGROUP;
           tuple_slot_id++) {
        txn_id_t tuple_txn_id = tile_group->GetHeader()->GetTransactionId(tuple_slot_id);
        if (tuple_txn_id != INVALID_TXN_ID) {
          PELOTON_ASSERT(tuple_txn_id == INITIAL_TXN_ID);
          std::unique_ptr<storage::Tuple> t(new storage::Tuple(schema, true));
          for (size_t col = 0; col < schema->GetColumnCount(); col++) {
            t->SetValue(col, tile_group->GetValue(tuple_slot_id, col), pool.get());
          }
          ItemPointer p(tile_group->GetTileGroupId(), tuple_slot_id);
          auto txn = concurrency::TransactionManagerFactory::GetInstance()
                  .BeginTransaction(IsolationLevelType::SERIALIZABLE);
          ItemPointer *ip = nullptr;
          table->InsertInIndexes(t.get(), p, txn, &ip);
          tile_group->GetHeader()->SetIndirection(tuple_slot_id, ip);
          concurrency::TransactionManagerFactory::GetInstance().CommitTransaction(
                  txn);
        }
      }
    }
  }

}

void WalRecovery::ReplayAllTxns(){

  for(auto it = commited_txns_.begin(); it!=commited_txns_.end(); it++){
    ReplaySingleTxn(it->first);
  }

  // reset epoch
  if (max_epoch_id_ != INVALID_EID) {
    auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
    epoch_manager.Reset(max_epoch_id_+1);
  }
}


void WalRecovery::ReplayLogFile(){

  size_t curr_txn_offset = 0;

  // Pass 1
  ParseFromDisk(ReplayStage::PASS_1);


  for(auto it = all_txns_.begin(); it!=all_txns_.end(); it++){

    if(it->second.first != LogRecordType::TRANSACTION_COMMIT)
      continue;

    auto offset_pair = std::make_pair(curr_txn_offset, curr_txn_offset);
    commited_txns_.insert(std::make_pair(it->first, offset_pair));

    curr_txn_offset += it->second.second;
  }

  // Pass 2
  log_buffer_  = new char[log_buffer_size_];
  ParseFromDisk(ReplayStage::PASS_2);

  ReplayAllTxns();

}



}
}