//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_checkpoint_manager.cpp
//
// Identification: src/logging/timestamp_checkpoint_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging/timestamp_checkpoint_manager.h"

#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/database_metrics_catalog.h"
#include "catalog/column.h"
#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/index_metrics_catalog.h"
#include "catalog/manager.h"
#include "catalog/language_catalog.h"
#include "catalog/proc_catalog.h"
#include "catalog/query_metrics_catalog.h"
#include "catalog/query_history_catalog.h"
#include "catalog/schema.h"
#include "catalog/settings_catalog.h"
#include "catalog/system_catalogs.h"
#include "catalog/table_catalog.h"
#include "catalog/table_metrics_catalog.h"
#include "catalog/trigger_catalog.h"
#include "common/container_tuple.h"
#include "common/timer.h"
#include "concurrency/timestamp_ordering_transaction_manager.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/insert_executor.h"
#include "index/index.h"
#include "index/index_factory.h"
#include "planner/insert_plan.h"
#include "settings/settings_manager.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "storage/storage_manager.h"
#include "storage/table_factory.h"
#include "storage/tile_group.h"
#include "storage/tile_group_factory.h"
#include "type/serializeio.h"
#include "type/type.h"

namespace peloton {
namespace logging {

void TimestampCheckpointManager::StartCheckpointing() {
  is_running_ = true;
  central_checkpoint_thread_.reset(
      new std::thread(&TimestampCheckpointManager::PerformCheckpointing, this));
}

void TimestampCheckpointManager::StopCheckpointing() {
  is_running_ = false;
  central_checkpoint_thread_->join();
}

bool TimestampCheckpointManager::DoCheckpointRecovery() {
  eid_t epoch_id = GetRecoveryCheckpointEpoch();
  if (epoch_id == INVALID_EID) {
    LOG_INFO("No checkpoint for recovery");
    return false;
  } else {
    LOG_INFO("Start checkpoint recovery");
    Timer<std::milli> recovery_timer;
    recovery_timer.Start();

    // recover catalog table checkpoint
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    if (!LoadCatalogTableCheckpoints(txn, epoch_id)) {
      LOG_ERROR("Catalog table checkpoint recovery was failed");
      txn_manager.AbortTransaction(txn);
      return false;
    }
    txn_manager.CommitTransaction(txn);

    // recover user table checkpoint
    txn = txn_manager.BeginTransaction();
    if (!LoadUserTableCheckpoints(txn, epoch_id)) {
      LOG_ERROR("User table checkpoint recovery was failed");
      txn_manager.AbortTransaction(txn);
      return false;
    }
    txn_manager.CommitTransaction(txn);

    // set recovered epoch id
    recovered_epoch_id_ = epoch_id;
    LOG_INFO("Complete checkpoint recovery for epoch %" PRIu64, epoch_id);

    recovery_timer.Stop();
    LOG_INFO("Checkpoint recovery time: %lf ms", recovery_timer.GetDuration());

    return true;
  }
}

eid_t TimestampCheckpointManager::GetRecoveryCheckpointEpoch() {
  // already recovered
  if (recovered_epoch_id_ != INVALID_EID) {
    return recovered_epoch_id_;
  }
  // for checkpoint recovery
  else {
    eid_t max_epoch = INVALID_EID;
    std::vector<std::string> dir_name_list;

    // Get list of checkpoint directories
    if (LoggingUtil::GetDirectoryList(checkpoint_base_dir_.c_str(),
                                      dir_name_list) == false) {
      LOG_ERROR("Failed to get directory list %s",
                checkpoint_base_dir_.c_str());
      return INVALID_EID;
    }

    // Get the newest epoch from checkpoint directories
    for (const auto dir_name : dir_name_list) {
      eid_t epoch_id;

      // get the directory name as epoch id
      // if it includes character, go next
      if ((epoch_id = std::strtoul(dir_name.c_str(), NULL, 10)) == 0) {
        continue;
      }

      if (epoch_id == INVALID_EID) {
        LOG_ERROR("Unexpected epoch value in checkpoints directory: %s",
                  dir_name.c_str());
      }

      // the largest epoch is a recovered epoch id
      if (epoch_id > max_epoch) {
        max_epoch = epoch_id;
      }
    }
    LOG_DEBUG("max epoch : %" PRIu64, max_epoch);
    return max_epoch;
  }
}

void TimestampCheckpointManager::PerformCheckpointing() {
  int count = checkpoint_interval_ - 1;
  while (1) {
    if (is_running_ == false) {
      LOG_INFO("Finish checkpoint thread");
      break;
    }
    // wait for interval
    std::this_thread::sleep_for(std::chrono::seconds(1));
    count++;
    if (count == checkpoint_interval_) {
      LOG_INFO("Start checkpointing");
      Timer<std::milli> checkpoint_timer;
      checkpoint_timer.Start();

      // create working checkpoint directory
      CreateWorkingCheckpointDirectory();

      // begin transaction and get epoch id as this checkpointing beginning
      auto &txn_manager =
          concurrency::TimestampOrderingTransactionManager::GetInstance(
              ProtocolType::TIMESTAMP_ORDERING,
              IsolationLevelType::SERIALIZABLE, ConflictAvoidanceType::WAIT);
      auto txn = txn_manager.BeginTransaction();
      cid_t begin_cid = txn->GetReadId();
      eid_t begin_epoch_id = txn->GetEpochId();

      // create checkpoint
      CreateCheckpoints(txn, begin_cid);

      // end transaction
      txn_manager.EndTransaction(txn);

      // finalize checkpoint directory:
      //   1) move working directory to epoch directory
      //   2) remove all of old checkpoints
      MoveWorkingToCheckpointDirectory(std::to_string(begin_epoch_id));
      RemoveOldCheckpoints(begin_epoch_id);

      checkpoint_timer.Stop();
      LOG_INFO(
          "Complete Checkpointing for epoch %" PRIu64 " (cid = %" PRIu64 ")",
          begin_epoch_id, begin_cid);
      LOG_INFO("Checkpointing time: %lf ms", checkpoint_timer.GetDuration());

      count = 0;
    }
  }
}

void TimestampCheckpointManager::CreateCheckpoints(
    concurrency::TransactionContext *txn,
    const cid_t begin_cid) {
  // prepare for data loading
  auto catalog = catalog::Catalog::GetInstance();
  auto storage_manager = storage::StorageManager::GetInstance();

  // do checkpointing to take tables into each file
  for (auto &db_catalog_entry_pair : catalog->GetDatabaseCatalogEntries(txn)) {
    auto &db_oid = db_catalog_entry_pair.first;
    auto &db_catalog_entry = db_catalog_entry_pair.second;
    auto database = storage_manager->GetDatabaseWithOid(db_oid);

    for (auto &table_catalog_entry_pair : db_catalog_entry->GetTableCatalogEntries()) {
      auto &table_oid = table_catalog_entry_pair.first;
      auto &table_catalog_entry = table_catalog_entry_pair.second;

      // make sure the table exists in this epoch
      // and system catalogs in catalog database are out of checkpoint
      if (table_catalog_entry == nullptr ||
          (db_oid == CATALOG_DATABASE_OID && (
              table_catalog_entry->GetTableOid() == SCHEMA_CATALOG_OID ||
              table_catalog_entry->GetTableOid() == TABLE_CATALOG_OID ||
              table_catalog_entry->GetTableOid() == COLUMN_CATALOG_OID ||
              table_catalog_entry->GetTableOid() == INDEX_CATALOG_OID ||
              table_catalog_entry->GetTableOid() == LAYOUT_CATALOG_OID))) {
        continue;
      }

      // create a checkpoint file for the table
      FileHandle file_handle;
      std::string file_name = GetWorkingCheckpointFileFullPath(
          db_catalog_entry->GetDatabaseName(), table_catalog_entry->GetSchemaName(),
          table_catalog_entry->GetTableName());
      if (LoggingUtil::OpenFile(file_name.c_str(), "wb", file_handle) != true) {
        LOG_ERROR("file open error: %s", file_name.c_str());
        return;
      }

      LOG_DEBUG("Checkpoint table %d '%s.%s' in database %d '%s'", table_oid,
                table_catalog_entry->GetSchemaName().c_str(),
                table_catalog_entry->GetTableName().c_str(),
                db_catalog_entry->GetDatabaseOid(),
                db_catalog_entry->GetDatabaseName().c_str());

      // insert data to checkpoint file
      // if table is catalog, then insert data without tile group info
      auto table = database->GetTableWithOid(table_oid);
      if (table_catalog_entry->GetSchemaName() == CATALOG_SCHEMA_NAME) {
        // settings_catalog_entry takes only persistent values
        if (table_catalog_entry->GetTableName() == "pg_settings") {
          CheckpointingTableDataWithPersistentCheck(table, begin_cid,
              table->GetSchema()->GetColumnID("is_persistent"),
              file_handle);
        } else {
          CheckpointingTableDataWithoutTileGroup(table, begin_cid, file_handle);
        }
      } else {
        CheckpointingTableData(table, begin_cid, file_handle);
      }

      LoggingUtil::CloseFile(file_handle);
    }  // end table loop

  }  // end database loop

}

void TimestampCheckpointManager::CheckpointingTableData(
    const storage::DataTable *table,
    const cid_t &begin_cid,
    FileHandle &file_handle) {
  CopySerializeOutput output_buffer;

  LOG_TRACE("Do checkpointing to table %d in database %d", table->GetOid(),
            table->GetDatabaseOid());

  // load all table data
  size_t tile_group_count = table->GetTileGroupCount();
  output_buffer.WriteLong(tile_group_count);
  LOG_TRACE("Tile group count: %lu", tile_group_count);
  for (oid_t tg_offset = START_OID; tg_offset < tile_group_count; tg_offset++) {
    auto tile_group = table->GetTileGroup(tg_offset);
    auto tile_group_header = tile_group->GetHeader();

    // write the tile group information
    output_buffer.WriteInt(tile_group->GetLayout().GetOid());
    output_buffer.WriteInt(tile_group->GetAllocatedTupleCount());

    LOG_TRACE("Tile group %u in %d \n%s", tile_group->GetTileGroupId(),
        table->GetOid(), tile_group->GetLayout().GetInfo().c_str());

    // collect and count visible tuples
    std::vector<oid_t> visible_tuples;
    oid_t max_tuple_count = tile_group->GetNextTupleSlot();
    oid_t column_count = table->GetSchema()->GetColumnCount();
    for (oid_t tuple_id = START_OID; tuple_id < max_tuple_count; tuple_id++) {
      if (IsVisible(tile_group_header, tuple_id, begin_cid)) {
        // load all field data of each column in the tuple
        output_buffer.WriteBool(true);
        for (oid_t column_id = START_OID; column_id < column_count; column_id++) {
          type::Value value = tile_group->GetValue(tuple_id, column_id);
          value.SerializeTo(output_buffer);
          LOG_TRACE("%s(column %d, tuple %d):%s\n", table->GetName().c_str(),
                    column_id, tuple_id, value.ToString().c_str());
        }
      } else {
        LOG_TRACE("%s's tuple %d is invisible\n", table->GetName().c_str(),
                  tuple_id);
      }
    }
    output_buffer.WriteBool(false);

    // write down tuple data to file
    int ret = fwrite((void *)output_buffer.Data(), output_buffer.Size(), 1,
                     file_handle.file);
    if (ret != 1) {
      LOG_ERROR("Write error");
      return;
    }

    output_buffer.Reset();
  }

  LoggingUtil::FFlushFsync(file_handle);
}

void TimestampCheckpointManager::CheckpointingTableDataWithoutTileGroup(
    const storage::DataTable *table, const cid_t &begin_cid,
    FileHandle &file_handle) {
  CopySerializeOutput output_buffer;

  LOG_TRACE("Do checkpointing without tile group to table %d in database %d",
            table->GetOid(), table->GetDatabaseOid());

  // load all table data without tile group information
  size_t tile_group_count = table->GetTileGroupCount();
  for (oid_t tg_offset = START_OID; tg_offset < tile_group_count; tg_offset++) {
    auto tile_group = table->GetTileGroup(tg_offset);
    auto tile_group_header = tile_group->GetHeader();

    // load visible tuples data in the table
    auto max_tuple_count = tile_group->GetNextTupleSlot();
    auto column_count = table->GetSchema()->GetColumnCount();
    for (oid_t tuple_id = START_OID; tuple_id < max_tuple_count; tuple_id++) {
      if (IsVisible(tile_group_header, tuple_id, begin_cid)) {
        // load all field data of each column in the tuple
        for (oid_t column_id = START_OID; column_id < column_count;
             column_id++) {
          type::Value value = tile_group->GetValue(tuple_id, column_id);
          value.SerializeTo(output_buffer);
          LOG_TRACE("%s(column %d, tuple %d):%s\n", table->GetName().c_str(),
                    column_id, tuple_id, value.ToString().c_str());
        }
      } else {
        LOG_TRACE("%s's tuple %d is invisible\n", table->GetName().c_str(),
                  tuple_id);
      }
    }

    // write down tuple data to file
    int ret = fwrite((void *)output_buffer.Data(), output_buffer.Size(), 1,
                     file_handle.file);
    if (ret != 1 && ret != 0) {
      LOG_ERROR("Write error: %d", ret);
      return;
    }

    output_buffer.Reset();
  }

  LoggingUtil::FFlushFsync(file_handle);
}

void TimestampCheckpointManager::CheckpointingTableDataWithPersistentCheck(
    const storage::DataTable *table, const cid_t &begin_cid,
    oid_t flag_column_id, FileHandle &file_handle) {
  CopySerializeOutput output_buffer;

  LOG_TRACE("Do checkpointing without tile group to table %d in database %d",
            table->GetOid(), table->GetDatabaseOid());

  // load all table data without tile group information
  size_t tile_group_count = table->GetTileGroupCount();
  for (oid_t tg_offset = START_OID; tg_offset < tile_group_count; tg_offset++) {
    auto tile_group = table->GetTileGroup(tg_offset);
    auto tile_group_header = tile_group->GetHeader();

    // load visible tuples data in the table
    auto max_tuple_count = tile_group->GetNextTupleSlot();
    auto column_count = table->GetSchema()->GetColumnCount();
    for (oid_t tuple_id = START_OID; tuple_id < max_tuple_count; tuple_id++) {
      // check visible and persistent flag.
      if (IsVisible(tile_group_header, tuple_id, begin_cid) &&
          tile_group->GetValue(tuple_id, flag_column_id).IsTrue()) {
        // load all field data of each column in the tuple
        for (oid_t column_id = START_OID; column_id < column_count;
             column_id++) {
          type::Value value = tile_group->GetValue(tuple_id, column_id);
          value.SerializeTo(output_buffer);
          LOG_TRACE("%s(column %d, tuple %d):%s\n", table->GetName().c_str(),
                    column_id, tuple_id, value.ToString().c_str());
        }
      } else {
        LOG_TRACE("%s's tuple %d is invisible\n", table->GetName().c_str(),
                  tuple_id);
      }
    }

    // write down tuple data to file
    int ret = fwrite((void *)output_buffer.Data(), output_buffer.Size(), 1,
                     file_handle.file);
    if (ret != 1 && ret != 0) {
      LOG_ERROR("Write error: %d", ret);
      return;
    }

    output_buffer.Reset();
  }

  LoggingUtil::FFlushFsync(file_handle);
}

bool TimestampCheckpointManager::IsVisible(
    const storage::TileGroupHeader *header, const oid_t &tuple_id,
    const cid_t &begin_cid) {
  txn_id_t tuple_txn_id = header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = header->GetEndCommitId(tuple_id);

  // the tuple has already been committed
  bool activated = (begin_cid >= tuple_begin_cid);
  // the tuple is not visible
  bool invalidated = (begin_cid >= tuple_end_cid);

  if (tuple_txn_id == INVALID_TXN_ID) {
    // this tuple is not available.
    return false;
  }

  if (tuple_txn_id == INITIAL_TXN_ID) {
    // this tuple is not owned by any other transaction.
    return activated && !invalidated;
  } else {
    // this tuple is owned by other transactions.
    if (tuple_begin_cid == MAX_CID) {
      // this tuple is an uncommitted version.
      return false;
    } else {
      return activated && !invalidated;
    }
  }
}

bool TimestampCheckpointManager::LoadCatalogTableCheckpoints(
    concurrency::TransactionContext *txn,
    const eid_t &epoch_id) {
  // prepare for catalog data file loading
  auto storage_manager = storage::StorageManager::GetInstance();
  auto catalog = catalog::Catalog::GetInstance();

  // first, recover all catalogs within catalog database
  auto catalog_db_catalog_entry = catalog->GetDatabaseCatalogEntry(txn, CATALOG_DATABASE_OID);
  PELOTON_ASSERT(catalog_db_catalog_entry != nullptr);
  for (auto &table_catalog_entry :
      catalog_db_catalog_entry->GetTableCatalogEntries((std::string)CATALOG_SCHEMA_NAME)) {
    if (!LoadCatalogTableCheckpoint(txn,
                                   epoch_id,
                                   catalog_db_catalog_entry->GetDatabaseOid(),
                                   table_catalog_entry->GetTableOid())) {
      return false;
    }
  }

  // recover all catalog tables within each database
  for (auto &db_catalog_entry_pair : catalog->GetDatabaseCatalogEntries(txn)) {
    auto &db_oid = db_catalog_entry_pair.first;
    auto &db_catalog_entry = db_catalog_entry_pair.second;

    // catalog database has already been recovered
    if (db_oid == CATALOG_DATABASE_OID) continue;

    // load system catalogs in the database
    // if not exist, then create the database storage object and catalogs
    if (storage_manager->HasDatabase(db_oid) == false) {
      LOG_DEBUG("Create database storage object %d '%s'", db_oid,
                db_catalog_entry->GetDatabaseName().c_str());
      LOG_DEBUG("And create system catalog objects for this database");

      // create database storage object
      auto database = new storage::Database(db_oid);
      // TODO: This should be deprecated, dbname should only exists in pg_db
      database->setDBName(db_catalog_entry->GetDatabaseName());
      storage_manager->AddDatabaseToStorageManager(database);

      // put database object into rw_object_set
      txn->RecordCreate(db_oid, INVALID_OID, INVALID_OID);

      // add core & non-core system catalog tables into database
      catalog->BootstrapSystemCatalogs(txn, database);
      catalog->catalog_map_[db_oid]->Bootstrap(txn,
                                               db_catalog_entry->GetDatabaseName());
    } else {
      LOG_DEBUG("Use existed database storage object %d '%s'", db_oid,
                db_catalog_entry->GetDatabaseName().c_str());
      LOG_DEBUG("And use its system catalog objects");
    }

    for (auto &table_catalog_entry :
         db_catalog_entry->GetTableCatalogEntries((std::string)CATALOG_SCHEMA_NAME)) {
      if (!LoadCatalogTableCheckpoint(txn,
                                      epoch_id,
                                      db_oid,
                                      table_catalog_entry->GetTableOid())) {
        return false;
      }
    }  // table loop end

  }  // database loop end

  return true;
}


bool TimestampCheckpointManager::LoadCatalogTableCheckpoint(
    concurrency::TransactionContext *txn,
    const eid_t &epoch_id,
    const oid_t db_oid,
    const oid_t table_oid) {
  auto catalog = catalog::Catalog::GetInstance();
  auto db_catalog_entry = catalog->GetDatabaseCatalogEntry(txn, db_oid);
  auto table_catalog_entry = db_catalog_entry->GetTableCatalogEntry(table_oid);
  auto &table_name = table_catalog_entry->GetTableName();
  auto system_catalogs = catalog->GetSystemCatalogs(db_oid);

  auto storage_manager = storage::StorageManager::GetInstance();
  auto table = storage_manager->GetTableWithOid(db_oid, table_oid);

  // load checkpoint files for catalog data.
  // except for catalogs that initialized in unsupported area:
  //   ColumnStatsCatalog, ZoneMapCatalog
  // TODO: Move these catalogs' initialization to catalog bootstrap
  // except for catalogs requiring to select recovered values:
  //   SettingsCatalog.
  // TODO: Implement a logic for selecting recovered values

  // catalogs out of recovery
  if (table_name == "pg_column_stats" || table_name == "zone_map" ||
      (db_oid == CATALOG_DATABASE_OID && (
          table_oid == SCHEMA_CATALOG_OID || table_oid == TABLE_CATALOG_OID ||
          table_oid == COLUMN_CATALOG_OID || table_oid == INDEX_CATALOG_OID ||
          table_oid == LAYOUT_CATALOG_OID || table_oid == CONSTRAINT_CATALOG_OID))) {
    // nothing to do (keep the default values, and not recover other data)
  } else {
    // read a checkpoint file for the catalog
    oid_t oid_align;
    FileHandle table_file;
    std::string table_filename = GetCheckpointFileFullPath(
        db_catalog_entry->GetDatabaseName(), table_catalog_entry->GetSchemaName(),
        table_name, epoch_id);
    if (!LoggingUtil::OpenFile(table_filename.c_str(), "rb", table_file)) {
      // Not existed here means that there is not the table in last checkpoint
      // because last checkpointing for the table was failed or
      // related settings like 'brain' was disabled in the checkpointing
      LOG_ERROR("Checkpoint file for catalog table %d '%s' is not existed",
                table_catalog_entry->GetTableOid(), table_name.c_str());
      return true;
    }

    LOG_DEBUG("Recover catalog table %d '%s.%s' in database %d '%s'",
              table_catalog_entry->GetTableOid(),
              table_catalog_entry->GetSchemaName().c_str(), table_name.c_str(),
              db_catalog_entry->GetDatabaseOid(),
              db_catalog_entry->GetDatabaseName().c_str());

    // catalogs with duplicate check
    // keep the embedded tuples which are stored in Peloton initialization,
    // but user tuples is recovered
    if (table_oid == DATABASE_CATALOG_OID || table_oid == SCHEMA_CATALOG_OID ||
        table_oid == TABLE_CATALOG_OID || table_oid == COLUMN_CATALOG_OID ||
        table_oid == INDEX_CATALOG_OID || table_oid == LAYOUT_CATALOG_OID ||
        table_oid == CONSTRAINT_CATALOG_OID || table_name == "pg_language" ||
        table_name == "pg_proc") {
      oid_align =
          RecoverTableDataWithDuplicateCheck(txn, table, table_file);
    }
    // catalogs with persistent check
    // recover settings which need to update embedded tuples with user set values.
    else if (table_name == "pg_settings") {
      std::vector<oid_t> key_column_ids = {table->GetSchema()->GetColumnID("name")};
      oid_t flag_column_id = table->GetSchema()->GetColumnID("is_persistent");
      oid_align =
          RecoverTableDataWithPersistentCheck(txn,
                                              table,
                                              table_file,
                                              key_column_ids,
                                              flag_column_id);
      // Reload settings data
      // TODO: settings::SettingsManager::GetInstance()->UpdateSettingListFromCatalog(txn)
    }
    // catalogs to be recovered without duplicate check
    // these catalog tables have no embedded tuples
    // Targets: pg_trigger, pg_*_metrics, pg_query_history, pg_column_stats, zone_map
    else {
      oid_align = RecoverTableDataWithoutTileGroup(txn, table, table_file);
    }

    LoggingUtil::CloseFile(table_file);

    // modify next OID of each catalog
    if (table_oid == DATABASE_CATALOG_OID) {
      catalog::DatabaseCatalog::GetInstance()->UpdateOid(oid_align);
    } else if (table_oid == SCHEMA_CATALOG_OID) {
      system_catalogs->GetSchemaCatalog()->UpdateOid(oid_align);
    } else if (table_oid == TABLE_CATALOG_OID) {
      system_catalogs->GetTableCatalog()->UpdateOid(oid_align);
    } else if (table_oid == COLUMN_CATALOG_OID) {
      system_catalogs->GetColumnCatalog()->UpdateOid(oid_align);
    } else if (table_oid == INDEX_CATALOG_OID) {
      system_catalogs->GetIndexCatalog()->UpdateOid(oid_align);
    } else if (table_oid == LAYOUT_CATALOG_OID) {
      // Layout OID is controlled within each DataTable object
    } else if (table_oid == CONSTRAINT_CATALOG_OID) {
      system_catalogs->GetConstraintCatalog()->UpdateOid(oid_align);
    } else if (table_name == "pg_proc") {
      catalog::ProcCatalog::GetInstance().UpdateOid(oid_align);
    } else if (table_name == "pg_trigger") {
      system_catalogs->GetTriggerCatalog()->UpdateOid(oid_align);
    } else if (table_name == "pg_language") {
      catalog::LanguageCatalog::GetInstance().UpdateOid(oid_align);
    } else if (table_name == "pg_settings" || table_name == "pg_database_metrics" ||
        table_name == "pg_table_metrics" || table_name == "pg_index_metrics" ||
        table_name == "pg_query_metrics" ||  table_name == "pg_query_history" ||
        table_name == "pg_column_stats" || table_name == "zone_map") {
      // OID is not used
    } else {
      LOG_ERROR("Unexpected catalog table appears: %s", table_name.c_str());
      PELOTON_ASSERT(false);
    }
  }

  return true;
}

bool TimestampCheckpointManager::LoadUserTableCheckpoints(
    concurrency::TransactionContext *txn,
    const eid_t &epoch_id) {
  // Recover table
  auto storage_manager = storage::StorageManager::GetInstance();
  auto catalog = catalog::Catalog::GetInstance();
  for (auto &db_catalog_entry_pair : catalog->GetDatabaseCatalogEntries(txn)) {
    auto &db_oid = db_catalog_entry_pair.first;
    auto database = storage_manager->GetDatabaseWithOid(db_oid);
    auto &db_catalog_entry = db_catalog_entry_pair.second;

    // the catalog database has already been recovered.
    if (db_oid == CATALOG_DATABASE_OID) continue;

    for (auto &table_catalog_entry_pair : db_catalog_entry->GetTableCatalogEntries()) {
      auto &table_oid = table_catalog_entry_pair.first;
      auto &table_catalog_entry = table_catalog_entry_pair.second;

      // catalog tables in each database have already been recovered
      if (table_catalog_entry->GetSchemaName() == CATALOG_SCHEMA_NAME) continue;

      // Recover storage object of the table
      if (!RecoverTableStorageObject(txn, db_oid, table_oid)) {
        LOG_ERROR("Storage object recovery for table %s failed",
                  table_catalog_entry->GetTableName().c_str());
        continue;
      }

      // read a checkpoint file for the catalog
      FileHandle table_file;
      std::string table_filename = GetCheckpointFileFullPath(
          db_catalog_entry->GetDatabaseName(), table_catalog_entry->GetSchemaName(),
          table_catalog_entry->GetTableName(), epoch_id);
      if (!LoggingUtil::OpenFile(table_filename.c_str(), "rb", table_file)) {
        LOG_ERROR("Checkpoint file for table %d '%s' is not existed", table_oid,
                  table_catalog_entry->GetTableName().c_str());
        continue;
      }

      LOG_DEBUG("Recover user table %d '%s.%s' in database %d '%s'", table_oid,
                table_catalog_entry->GetSchemaName().c_str(),
                table_catalog_entry->GetTableName().c_str(),
                db_catalog_entry->GetDatabaseOid(),
                db_catalog_entry->GetDatabaseName().c_str());

      // recover the table from the checkpoint file
      RecoverTableData(txn, database->GetTableWithOid(table_oid), table_file);

      LoggingUtil::CloseFile(table_file);

    }  // table loop end

    // register foreign key to sink table
    for (auto &table_catalog_entry_pair : db_catalog_entry->GetTableCatalogEntries()) {
      auto source_table_schema =
          database->GetTableWithOid(table_catalog_entry_pair.first)->GetSchema();
      if (source_table_schema->HasForeignKeys()) {
        for(auto constraint : source_table_schema->GetForeignKeyConstraints()) {
          auto sink_table = database->GetTableWithOid(constraint->GetFKSinkTableOid());
          sink_table->GetSchema()->RegisterForeignKeySource(constraint);
        }
      }
    }

  }  // database loop end

  return true;
}

bool TimestampCheckpointManager::RecoverTableStorageObject(
    concurrency::TransactionContext *txn,
    const oid_t db_oid,
    const oid_t table_oid) {
  auto database =
      storage::StorageManager::GetInstance()->GetDatabaseWithOid(db_oid);
  auto table_catalog_entry =
      catalog::Catalog::GetInstance()->GetTableCatalogEntry(txn, db_oid, table_oid);

  // This function targets user tables rather than catalogs
  PELOTON_ASSERT(db_oid != CATALOG_DATABASE_OID);
  PELOTON_ASSERT(table_catalog_entry->GetSchemaName() != CATALOG_SCHEMA_NAME);

  LOG_DEBUG("Create table storage object %d '%s.%s'", table_oid,
            table_catalog_entry->GetSchemaName().c_str(),
            table_catalog_entry->GetTableName().c_str());

  // recover column information
  std::vector<catalog::Column> columns;
  for (auto column_catalog_entry_pair : table_catalog_entry->GetColumnCatalogEntries()) {
    auto column_oid = column_catalog_entry_pair.first;
    auto column_catalog_entry = column_catalog_entry_pair.second;

    // create column storage object
    auto column = catalog::Column(column_catalog_entry->GetColumnType(),
                                  column_catalog_entry->GetColumnLength(),
                                  column_catalog_entry->GetColumnName(),
                                  column_catalog_entry->IsInlined(),
                                  column_catalog_entry->GetColumnOffset());

    // recover column constraints: NOT NULL
    if (column_catalog_entry->IsNotNull()) {
      column.SetNotNull();
    }

    // recover column constraints: DEFAULT
    if (column_catalog_entry->HasDefault()) {
      column.SetDefaultValue(column_catalog_entry->GetDefaultValue());
    }

    // Set a column into the vector in order of the column_oid.
    // Cannot use push_back of vector because column_catalog_entries doesn't acquire
    // the column info in the order from pg_attribute.
    auto column_itr = columns.begin();
    for (oid_t idx_count = START_OID; idx_count < column_oid; idx_count++) {
      if (column_itr == columns.end() ||
          column_itr->GetOffset() > column.GetOffset()) {
          break;
      } else {
        column_itr++;
      }
    }
    columns.insert(column_itr, column);;

  } // column loop end

  // create schema for the table
  std::unique_ptr<catalog::Schema> schema(new catalog::Schema(columns));

  // recover default layout
  auto default_layout =
      table_catalog_entry->GetLayout(table_catalog_entry->GetDefaultLayoutOid());

  // create table storage object.
  // if the default layout type is hybrid, set default layout separately
  bool own_schema = true;
  bool adapt_table = false;
  bool is_catalog = false;
  storage::DataTable *table;
  if (default_layout->IsHybridStore()) {
    table =
        storage::TableFactory::GetDataTable(db_oid,
                                            table_oid,
                                            schema.release(),
                                            table_catalog_entry->GetTableName(),
                                            DEFAULT_TUPLES_PER_TILEGROUP,
                                            own_schema,
                                            adapt_table,
                                            is_catalog);
    table->SetDefaultLayout(default_layout);
    // adjust layout oid value
    table->GetNextLayoutOid();
  } else {
    table =
        storage::TableFactory::GetDataTable(db_oid,
                                            table_oid,
                                            schema.release(),
                                            table_catalog_entry->GetTableName(),
                                            DEFAULT_TUPLES_PER_TILEGROUP,
                                            own_schema,
                                            adapt_table,
                                            is_catalog,
                                            default_layout->GetLayoutType());
  }
  database->AddTable(table, is_catalog);

  // put data table object into rw_object_set
  txn->RecordCreate(db_oid, table_oid, INVALID_OID);

  // recover triggers of the storage table
  table->UpdateTriggerListFromCatalog(txn);

  // recover index storage objects
  for (auto &index_catalog_entry_pair :
          table_catalog_entry->GetIndexCatalogEntries()) {
    auto index_oid = index_catalog_entry_pair.first;
    auto index_catalog_entry = index_catalog_entry_pair.second;

    LOG_TRACE(
        "|- Index %d '%s':  Index type %s, Index constraint %s, unique "
        "keys %d",
        index_oid, index_catalog_entry->GetIndexName().c_str(),
        IndexTypeToString(index_catalog_entry->GetIndexType()).c_str(),
        IndexConstraintTypeToString(index_catalog_entry->GetIndexConstraint())
            .c_str(),
        index_catalog_entry->HasUniqueKeys());

    auto &key_attrs = index_catalog_entry->GetKeyAttrs();
    auto key_schema =
        catalog::Schema::CopySchema(table->GetSchema(), key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    // Set index metadata
    auto index_metadata =
        new index::IndexMetadata(index_catalog_entry->GetIndexName(),
                                 index_oid,
                                 table_oid,
                                 db_oid,
                                 index_catalog_entry->GetIndexType(),
                                 index_catalog_entry->GetIndexConstraint(),
                                 table->GetSchema(),
                                 key_schema,
                                 key_attrs,
                                 index_catalog_entry->HasUniqueKeys());

    // create index storage objects and add it to the table
    std::shared_ptr<index::Index> index(
        index::IndexFactory::GetIndex(index_metadata));
    table->AddIndex(index);

    // Put index object into rw_object_set
    txn->RecordCreate(db_oid, table_oid, index_oid);

  }  // index loop end

  // recover table constraints
  for (auto constraint_catalog_entry_pair :
          table_catalog_entry->GetConstraintCatalogEntries()) {
    auto constraint_oid = constraint_catalog_entry_pair.first;
    auto constraint_catalog_entry = constraint_catalog_entry_pair.second;

    // create constraint
    std::shared_ptr<catalog::Constraint> constraint;
    switch (constraint_catalog_entry->GetConstraintType()) {
      case ConstraintType::PRIMARY:
      case ConstraintType::UNIQUE: {
        constraint =
            std::make_shared<catalog::Constraint>(constraint_oid,
                                                  constraint_catalog_entry->GetConstraintType(),
                                                  constraint_catalog_entry->GetConstraintName(),
                                                  constraint_catalog_entry->GetTableOid(),
                                                  constraint_catalog_entry->GetColumnIds(),
                                                  constraint_catalog_entry->GetIndexOid());
        break;
      }
      case ConstraintType::FOREIGN: {
        constraint =
            std::make_shared<catalog::Constraint>(constraint_oid,
                                                  constraint_catalog_entry->GetConstraintType(),
                                                  constraint_catalog_entry->GetConstraintName(),
                                                  constraint_catalog_entry->GetTableOid(),
                                                  constraint_catalog_entry->GetColumnIds(),
                                                  constraint_catalog_entry->GetIndexOid(),
                                                  constraint_catalog_entry->GetFKSinkTableOid(),
                                                  constraint_catalog_entry->GetFKSinkColumnIds(),
                                                  constraint_catalog_entry->GetFKUpdateAction(),
                                                  constraint_catalog_entry->GetFKDeleteAction());
        // cannot register foreign key into a sink table here
        // because the sink table might not be recovered yet.
        break;
      }
      case ConstraintType::CHECK: {
        constraint =
            std::make_shared<catalog::Constraint>(constraint_oid,
                                                  constraint_catalog_entry->GetConstraintType(),
                                                  constraint_catalog_entry->GetConstraintName(),
                                                  constraint_catalog_entry->GetTableOid(),
                                                  constraint_catalog_entry->GetColumnIds(),
                                                  constraint_catalog_entry->GetIndexOid(),
                                                  constraint_catalog_entry->GetCheckExp());
        break;
      }
      default:
        LOG_ERROR("Unexpected constraint type is appeared: %s",
                  ConstraintTypeToString(constraint_catalog_entry->GetConstraintType()).c_str());
        break;
    }

    LOG_TRACE("|- %s", constraint->GetInfo().c_str());

    // set the constraint into the table
    table->GetSchema()->AddConstraint(constraint);

  } // constraint loop end


  return true;
}

void TimestampCheckpointManager::RecoverTableData(
    concurrency::TransactionContext *txn,
    storage::DataTable *table,
    FileHandle &file_handle) {
  size_t table_size = LoggingUtil::GetFileSize(file_handle);
  if (table_size == 0) return;
  std::unique_ptr<char[]> data(new char[table_size]);
  if (!LoggingUtil::ReadNBytesFromFile(file_handle, data.get(), table_size)) {
    LOG_ERROR("Checkpoint table file read error");
    return;
  }
  CopySerializeInput input_buffer(data.get(), table_size);

  LOG_TRACE("Recover table %d data (%lu byte)", table->GetOid(), table_size);

  // Drop all default tile groups created by table catalog recovery
  table->DropTileGroups();

  // Create tile group
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());
  auto schema = table->GetSchema();
  auto default_layout = table->GetDefaultLayout();
  auto column_count = schema->GetColumnCount();
  oid_t tile_group_count = input_buffer.ReadLong();
  for (oid_t tg_idx = START_OID; tg_idx < tile_group_count; tg_idx++) {
    // recover layout for this tile group
    oid_t layout_oid = input_buffer.ReadInt();
    std::shared_ptr<const storage::Layout> layout;
    if (default_layout->GetOid() != layout_oid) {
      layout = catalog::Catalog::GetInstance()
            ->GetTableCatalogEntry(txn, table->GetDatabaseOid(), table->GetOid())
            ->GetLayout(layout_oid);
    } else {
      layout = default_layout;
    }

    // The tile_group_id can't be recovered.
    // Because if active_tile_group_count_ in DataTable class is changed after
    // restart (e.g. in case of change of connection_thread_count setting),
    // then a recovered tile_group_id might get collision with a tile_group_id
    // which set for the default tile group of a table.
    oid_t tile_group_id =
        storage::StorageManager::GetInstance()->GetNextTileGroupId();
    oid_t allocated_tuple_count = input_buffer.ReadInt();

    // recover tile group
    auto layout_schemas = layout->GetLayoutSchemas(schema);
    std::shared_ptr<storage::TileGroup> tile_group(
        storage::TileGroupFactory::GetTileGroup(table->GetDatabaseOid(),
                                                table->GetOid(),
                                                tile_group_id,
                                                table,
                                                layout_schemas,
                                                layout,
                                                allocated_tuple_count));

    LOG_TRACE("Recover tile group %u in %d \n%s", tile_group->GetTileGroupId(),
        table->GetOid(), tile_group->GetLayout().GetInfo().c_str());

    // add the tile group to table
    oid_t active_tile_group_id = tg_idx % table->GetActiveTileGroupCount();
    table->AddTileGroup(tile_group, active_tile_group_id);

    // recover tuples located in the tile group
    while (input_buffer.ReadBool()) {
      // recover values on each column
      std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
      for (oid_t column_id = 0; column_id < column_count; column_id++) {
        auto value = type::Value::DeserializeFrom(input_buffer,
                                                  schema->GetType(column_id),
                                                  NULL);
        tuple->SetValue(column_id, value, pool.get());
      }

      // insert the tuple into the tile group
      oid_t tuple_slot = tile_group->InsertTuple(tuple.get());
      ItemPointer location(tile_group->GetTileGroupId(), tuple_slot);
      if (location.block != INVALID_OID) {
        // register the location of the inserted tuple to the table without
        // foreign key check to avoid an error which occurs in tables with
        // the mutual foreign keys each other
        ItemPointer *index_entry_ptr = nullptr;
        if (table->InsertTuple(tuple.get(),
                               location,
                               txn,
                               &index_entry_ptr,
                               false)) {
          concurrency::TransactionManagerFactory::GetInstance().PerformInsert(
              txn, location, index_entry_ptr);
        } else {
          LOG_ERROR("Tuple insert error for table %d", table->GetOid());
        }
      } else {
        LOG_ERROR("Tuple insert error for tile group %d of table %d",
                  tile_group->GetTileGroupId(), table->GetOid());
      }

    }  // tuple loop end

  }  // tile group loop end

  // if # of recovered tile_groups is smaller than # of active tile
  // groups, then create default tile group for rest of the slots of
  // active tile groups
  for (auto active_tile_group_id = tile_group_count;
      active_tile_group_id < table->GetActiveTileGroupCount();
      active_tile_group_id++) {
    table->AddDefaultTileGroup(active_tile_group_id);
  }
}

oid_t TimestampCheckpointManager::RecoverTableDataWithoutTileGroup(
    concurrency::TransactionContext *txn,
    storage::DataTable *table,
    FileHandle &file_handle) {
  size_t table_size = LoggingUtil::GetFileSize(file_handle);
  if (table_size == 0) return 0;
  std::unique_ptr<char[]> data(new char[table_size]);
  if (!LoggingUtil::ReadNBytesFromFile(file_handle, data.get(), table_size)) {
    LOG_ERROR("Checkpoint table file read error");
    return 0;
  }
  CopySerializeInput input_buffer(data.get(), table_size);

  LOG_TRACE("Recover table %d data without tile group (%lu byte)",
            table->GetOid(), table_size);

  // recover table tuples
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());
  oid_t insert_tuple_count = 0;
  auto schema = table->GetSchema();
  oid_t column_count = schema->GetColumnCount();
  while (input_buffer.RestSize() > 0) {
    // recover values on each column
    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
    ItemPointer *index_entry_ptr = nullptr;
    for (oid_t column_id = 0; column_id < column_count; column_id++) {
      auto value = type::Value::DeserializeFrom(input_buffer,
                                                schema->GetType(column_id),
                                                NULL);
      tuple->SetValue(column_id, value, pool.get());
    }

    // insert tuple into the table without foreign key check to avoid an error
    // which occurs in tables with the mutual foreign keys each other
    ItemPointer location =
        table->InsertTuple(tuple.get(), txn, &index_entry_ptr, false);
    if (location.block != INVALID_OID) {
      concurrency::TransactionManagerFactory::GetInstance().PerformInsert(
          txn, location, index_entry_ptr);
      insert_tuple_count++;
    } else {
      LOG_ERROR("Tuple insert error for table %d", table->GetOid());
    }
  }

  return insert_tuple_count;
}

oid_t TimestampCheckpointManager::RecoverTableDataWithDuplicateCheck(
    concurrency::TransactionContext *txn,
    storage::DataTable *table,
    FileHandle &file_handle) {
  size_t table_size = LoggingUtil::GetFileSize(file_handle);
  if (table_size == 0) return 0;
  std::unique_ptr<char[]> data(new char[table_size]);
  if (!LoggingUtil::ReadNBytesFromFile(file_handle, data.get(), table_size)) {
    LOG_ERROR("Checkpoint table file read error");
    return 0;
  }
  CopySerializeInput input_buffer(data.get(), table_size);

  LOG_TRACE("Recover table %d data with duplicate check (%lu byte)",
            table->GetOid(), table_size);

  // look for all primary key columns
  std::vector<oid_t> pk_columns;
  auto schema = table->GetSchema();
  PELOTON_ASSERT(schema->HasPrimary());
  oid_t column_count = schema->GetColumnCount();
  for (auto constraint : schema->GetConstraints()) {
    if (constraint.second->GetType() == ConstraintType::PRIMARY) {
      pk_columns = constraint.second->GetColumnIds();
    }
  }

  // recover table tuples
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());
  oid_t insert_tuple_count = 0;
  while (input_buffer.RestSize() > 0) {
    // recover values on each column
    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
    ItemPointer *index_entry_ptr = nullptr;
    for (oid_t column_id = 0; column_id < column_count; column_id++) {
      auto value = type::Value::DeserializeFrom(input_buffer,
                                                schema->GetType(column_id),
                                                NULL);
      tuple->SetValue(column_id, value, pool.get());
    }

    // duplicate check
    // if all primary key values are existed, the tuple is not stored in the
    // table
    bool duplicated = false;
    for (oid_t tg_offset = 0; tg_offset < table->GetTileGroupCount();
         tg_offset++) {
      auto tile_group = table->GetTileGroup(tg_offset);
      auto max_tuple_count = tile_group->GetNextTupleSlot();
      for (oid_t tuple_id = 0; tuple_id < max_tuple_count; tuple_id++) {
        // check all primary key columns
        bool check_all_pk_values_same = true;
        for (auto pk_column : pk_columns) {
          if (tile_group->GetValue(tuple_id, pk_column)
                        .CompareNotEquals(tuple->GetValue(pk_column))
              == CmpBool::CmpTrue) {
            check_all_pk_values_same = false;
            break;
          }
        }
        if (check_all_pk_values_same) {
          duplicated = true;
          break;
        }
      }
    }

    // if not duplicated, insert the tuple
    if (!duplicated) {
      // insert tuple into the table without foreign key check to avoid an error
      // which occurs in tables with the mutual foreign keys each other
      ItemPointer location =
          table->InsertTuple(tuple.get(), txn, &index_entry_ptr, false);
      if (location.block != INVALID_OID) {
        concurrency::TransactionManagerFactory::GetInstance().PerformInsert(
            txn, location, index_entry_ptr);
        insert_tuple_count++;
      } else {
        LOG_ERROR("Tuple insert error for table %d", table->GetOid());
      }
    }
  }

  return insert_tuple_count;
}

oid_t TimestampCheckpointManager::RecoverTableDataWithPersistentCheck(
    concurrency::TransactionContext *txn,
    storage::DataTable *table,
    FileHandle &file_handle,
    std::vector<oid_t> key_colmun_ids,
    oid_t flag_column_id) {
  size_t table_size = LoggingUtil::GetFileSize(file_handle);
  if (table_size == 0) return 0;
  std::unique_ptr<char[]> data(new char[table_size]);
  if (!LoggingUtil::ReadNBytesFromFile(file_handle, data.get(), table_size)) {
    LOG_ERROR("Checkpoint table file read error");
    return 0;
  }
  CopySerializeInput input_buffer(data.get(), table_size);

  LOG_TRACE("Recover table %d data with persistent check (%lu byte)",
            table->GetOid(), table_size);

  // recover table tuples
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());
  oid_t insert_tuple_count = 0;
  auto schema = table->GetSchema();
  oid_t column_count = schema->GetColumnCount();
  while (input_buffer.RestSize() > 0) {
    // recover values on each column
    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
    ItemPointer *index_entry_ptr = nullptr;
    for (oid_t column_id = 0; column_id < column_count; column_id++) {
      auto value = type::Value::DeserializeFrom(input_buffer,
                                                schema->GetType(column_id),
                                                NULL);
      tuple->SetValue(column_id, value, pool.get());
    }

    // Persistent check
    // If same name tuple exists in the table already, delete its tuple.
    // If the tuple's persistent is changed to false, then recovered tuple
    // is discarded
    storage::Tuple target_tuple;
    bool do_insert = true;
    for (oid_t tg_offset = 0; tg_offset < table->GetTileGroupCount();
         tg_offset++) {
      auto tile_group = table->GetTileGroup(tg_offset);
      auto max_tuple_count = tile_group->GetNextTupleSlot();
      for (oid_t tuple_id = 0; tuple_id < max_tuple_count; tuple_id++) {
        // check all key columns which identify the tuple
        bool check_all_values_same = true;
        for (auto key_column : key_colmun_ids) {
          if (tile_group->GetValue(tuple_id, key_column)
                        .CompareNotEquals(tuple->GetValue(key_column))
              == CmpBool::CmpTrue) {
            check_all_values_same = false;
            break;
          }
        }

        // If found a same tuple and its parameter is persistent,
        // then update it,
        if (check_all_values_same &&
          tile_group->GetValue(tuple_id, flag_column_id).IsTrue()) {
          for (oid_t column_id = 0; column_id < column_count; column_id++) {
            auto new_value = tuple->GetValue(column_id);
            tile_group->SetValue(new_value, tuple_id, column_id);
          }
          do_insert = false;
          break;
        }
      }
    }

    // If there is no same tuple in the table, insert it.
    if (do_insert) {
      // insert tuple into the table without foreign key check to avoid an error
      // which occurs in tables with the mutual foreign keys each other
      ItemPointer location =
          table->InsertTuple(tuple.get(), txn, &index_entry_ptr, false);
      if (location.block != INVALID_OID) {
        concurrency::TransactionManagerFactory::GetInstance().PerformInsert(
            txn, location, index_entry_ptr);
        insert_tuple_count++;
      } else {
        LOG_ERROR("Tuple insert error for table %d", table->GetOid());
      }
    }
  }

  return insert_tuple_count;
}

}  // namespace logging
}  // namespace peloton
