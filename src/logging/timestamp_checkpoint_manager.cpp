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
#include "catalog/foreign_key.h"
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
    if (LoadCatalogTableCheckpoint(epoch_id) == false) {
      LOG_ERROR("Catalog table checkpoint recovery was failed");
      return false;
    }

    // recover user table checkpoint
    if (LoadUserTableCheckpoint(epoch_id) == false) {
      LOG_ERROR("User table checkpoint recovery was failed");
      return false;
    }

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
      CreateCheckpoint(begin_cid, txn);

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

void TimestampCheckpointManager::CreateCheckpoint(
    const cid_t begin_cid, concurrency::TransactionContext *txn) {
  // prepare for data loading
  auto catalog = catalog::Catalog::GetInstance();
  auto storage_manager = storage::StorageManager::GetInstance();

  // do checkpointing to take tables into each file
  for (auto db_catalog_pair : catalog->GetDatabaseObjects(txn)) {
    auto db_oid = db_catalog_pair.first;
    auto db_catalog = db_catalog_pair.second;
    auto database = storage_manager->GetDatabaseWithOid(db_oid);

    for (auto table_catalog_pair : db_catalog->GetTableObjects()) {
      auto table_oid = table_catalog_pair.first;
      auto table_catalog = table_catalog_pair.second;

      // make sure the table exists in this epoch
      // and system catalogs in catalog database are out of checkpoint
    	if (table_catalog == nullptr ||
					(db_oid == CATALOG_DATABASE_OID && (
		    			table_catalog->GetTableOid() == SCHEMA_CATALOG_OID ||
		    			table_catalog->GetTableOid() == TABLE_CATALOG_OID ||
		    			table_catalog->GetTableOid() == COLUMN_CATALOG_OID ||
		    			table_catalog->GetTableOid() == INDEX_CATALOG_OID ||
		    			table_catalog->GetTableOid() == LAYOUT_CATALOG_OID))) {
    		continue;
    	}

      // create a checkpoint file for the table
      FileHandle file_handle;
      std::string file_name = GetWorkingCheckpointFileFullPath(
          db_catalog->GetDatabaseName(), table_catalog->GetSchemaName(),
          table_catalog->GetTableName());
      if (LoggingUtil::OpenFile(file_name.c_str(), "wb", file_handle) != true) {
        LOG_ERROR("file open error: %s", file_name.c_str());
        return;
      }

      LOG_DEBUG("Checkpoint table %d '%s.%s' in database %d '%s'", table_oid,
                table_catalog->GetSchemaName().c_str(),
                table_catalog->GetTableName().c_str(),
                db_catalog->GetDatabaseOid(),
                db_catalog->GetDatabaseName().c_str());

      // insert data to checkpoint file
      // if table is catalog, then insert data without tile group info
      auto table = database->GetTableWithOid(table_oid);
      if (table_catalog->GetSchemaName() == CATALOG_SCHEMA_NAME) {
        CheckpointingTableDataWithoutTileGroup(table, begin_cid, file_handle);
      } else {
        CheckpointingTableData(table, begin_cid, file_handle);
      }

      LoggingUtil::CloseFile(file_handle);
    }  // end table loop

  }  // end database loop

  // do checkpointing to storage object
  FileHandle metadata_file;
  std::string metadata_filename = GetWorkingMetadataFileFullPath();
  if (LoggingUtil::OpenFile(metadata_filename.c_str(), "wb", metadata_file) !=
      true) {
    LOG_ERROR("Create catalog file failed!");
    return;
  }
  CheckpointingStorageObject(metadata_file, txn);
  LoggingUtil::CloseFile(metadata_file);
}

void TimestampCheckpointManager::CheckpointingTableData(
    const storage::DataTable *table, const cid_t &begin_cid,
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

    // serialize the tile group structure
    tile_group->SerializeTo(output_buffer);

    LOG_TRACE("Deserialized tile group %u in %s \n%s", tile_group->GetTileGroupId(),
    		table->GetName().c_str(), tile_group->GetLayout().GetInfo().c_str());

    // collect and count visible tuples
    std::vector<oid_t> visible_tuples;
    oid_t max_tuple_count = tile_group->GetNextTupleSlot();
    oid_t column_count = table->GetSchema()->GetColumnCount();
    for (oid_t tuple_id = START_OID; tuple_id < max_tuple_count; tuple_id++) {
      if (IsVisible(tile_group_header, tuple_id, begin_cid)) {
        visible_tuples.push_back(tuple_id);
      } else {
        LOG_TRACE("%s's tuple %d is invisible\n", table->GetName().c_str(),
                  tuple_id);
      }
    }
    output_buffer.WriteLong(visible_tuples.size());
    LOG_TRACE("Tuple count in tile group %d: %lu", tile_group->GetTileGroupId(),
              visible_tuples.size());

    // load visible tuples data in the table
    for (auto tuple_id : visible_tuples) {
      // load all field data of each column in the tuple
      for (oid_t column_id = START_OID; column_id < column_count; column_id++) {
        type::Value value = tile_group->GetValue(tuple_id, column_id);
        value.SerializeTo(output_buffer);
        LOG_TRACE("%s(column %d, tuple %d):%s\n", table->GetName().c_str(),
                  column_id, tuple_id, value.ToString().c_str());
      }
    }

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

// TODO: Integrate this function to CreateCatalogTableCheckpoint,
//       after all necessary catalog data to recover all storage data
//       is stored into catalog table. (Not serialize storage data for catalog)
void TimestampCheckpointManager::CheckpointingStorageObject(
    FileHandle &file_handle, concurrency::TransactionContext *txn) {
  CopySerializeOutput metadata_buffer;
  auto catalog = catalog::Catalog::GetInstance();
  LOG_TRACE("Do checkpointing to storage objects");

  // insert each database information into metadata file
  auto storage_manager = storage::StorageManager::GetInstance();
  auto db_catalogs = catalog->GetDatabaseObjects(txn);
  metadata_buffer.WriteLong(db_catalogs.size() - 1);
  for (auto db_catalog_pair : db_catalogs) {
    auto db_oid = db_catalog_pair.first;
    auto db_catalog = db_catalog_pair.second;

    // except for catalog database
    if (db_oid == CATALOG_DATABASE_OID) continue;

    LOG_TRACE("Write database storage object %d '%s'", db_oid,
              db_catalog->GetDatabaseName().c_str());

    // write database information
    metadata_buffer.WriteInt(db_oid);

    // eliminate catalog tables from table catalog objects in the database
    std::vector<std::shared_ptr<catalog::TableCatalogObject>> table_catalogs;
    auto all_table_catalogs = db_catalog->GetTableObjects();
    for (auto table_catalog_pair : all_table_catalogs) {
      auto table_catalog = table_catalog_pair.second;
      if (table_catalog->GetSchemaName() != CATALOG_SCHEMA_NAME) {
        table_catalogs.push_back(table_catalog);
      }
    }

    // insert each table information in the database into metadata file
    metadata_buffer.WriteLong(table_catalogs.size());
    for (auto table_catalog : table_catalogs) {
      auto table_oid = table_catalog->GetTableOid();
      auto table = storage_manager->GetTableWithOid(db_oid, table_oid);
      auto schema = table->GetSchema();

      LOG_DEBUG(
          "Write table storage object %d '%s' (%lu columns) in database "
          "%d '%s'",
          table_oid, table_catalog->GetTableName().c_str(),
          schema->GetColumnCount(), db_oid,
          db_catalog->GetDatabaseName().c_str());

      // write table information
      metadata_buffer.WriteInt(table_oid);

      // Write schema information
      auto column_catalogs = table_catalog->GetColumnObjects();
      metadata_buffer.WriteLong(column_catalogs.size());
      for (auto column_catalog_pair : column_catalogs) {
        auto column_oid = column_catalog_pair.first;
        auto column_catalog = column_catalog_pair.second;
        auto column = schema->GetColumn(column_oid);

        // write column information
        // ToDo: column length should be contained in column catalog
        metadata_buffer.WriteInt(column_oid);
        metadata_buffer.WriteLong(column.GetLength());

        // Column constraints
        // ToDo: Constraints should be contained in catalog
        auto constraints = column.GetConstraints();
        metadata_buffer.WriteLong(constraints.size());
        for (auto constraint : constraints) {
          constraint.SerializeTo(metadata_buffer);
        }
      }

      // Write schema information (multi-column constraints)
      // ToDo: Multi-constraints should be contained in catalog
      auto multi_constraints = schema->GetMultiConstraints();
      metadata_buffer.WriteLong(multi_constraints.size());
      for (auto multi_constraint : multi_constraints) {
        multi_constraint.SerializeTo(metadata_buffer);
      }

      // Write foreign key information of this sink table
      // ToDo: Foreign key should be contained in catalog
      auto foreign_key_count = table->GetForeignKeyCount();
      metadata_buffer.WriteLong(foreign_key_count);
      for (oid_t fk_idx = 0; fk_idx < foreign_key_count; fk_idx++) {
        auto foreign_key = table->GetForeignKey(fk_idx);
        foreign_key->SerializeTo(metadata_buffer);
      }

      // Write foreign key information of this source tables
      // ToDo: Foreign key should be contained in catalog
      auto foreign_key_src_count = table->GetForeignKeySrcCount();
      metadata_buffer.WriteLong(foreign_key_src_count);
      for (oid_t fk_src_idx = 0; fk_src_idx < foreign_key_src_count;
           fk_src_idx++) {
        auto foreign_key_src = table->GetForeignKeySrc(fk_src_idx);
        foreign_key_src->SerializeTo(metadata_buffer);
      }

      // Nothing to write about index

    }  // table loop end

  }  // database loop end

  // Output data to file
  int ret = fwrite((void *)metadata_buffer.Data(), metadata_buffer.Size(), 1,
                   file_handle.file);
  if (ret != 1) {
    LOG_ERROR("Checkpoint metadata file write error");
    return;
  }
  LoggingUtil::FFlushFsync(file_handle);
}

bool TimestampCheckpointManager::LoadCatalogTableCheckpoint(const eid_t &epoch_id) {
  // prepare for catalog data file loading
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto catalog = catalog::Catalog::GetInstance();

  // first, recover all catalogs within catalog database
  auto catalog_db_catalog = catalog->GetDatabaseObject(CATALOG_DATABASE_OID, txn);
  PELOTON_ASSERT(catalog_db_catalog != nullptr);
  for (auto table_catalog :
  		catalog_db_catalog->GetTableObjects((std::string)CATALOG_SCHEMA_NAME)) {
    if (LoadCatalogTableCheckpoint(epoch_id, catalog_db_catalog->GetDatabaseOid(),
    		table_catalog->GetTableOid(), txn) == false) {
      txn_manager.AbortTransaction(txn);
    	return false;
    }
  }

  // recover all catalog table within each database
  for (auto db_catalog_pair : catalog->GetDatabaseObjects(txn)) {
    auto db_oid = db_catalog_pair.first;
    auto db_catalog = db_catalog_pair.second;

    // catalog database has already been recovered
    if (db_oid == CATALOG_DATABASE_OID) continue;

    // load system catalogs in the database
    // if not exist, then create the database storage object and catalogs
    if (storage_manager->HasDatabase(db_oid) == false) {
      LOG_DEBUG("Create database storage object %d '%s'", db_oid,
                db_catalog->GetDatabaseName().c_str());
      LOG_DEBUG("And create system catalog objects for this database");

      // create database storage object
      auto database = new storage::Database(db_oid);
      // TODO: This should be deprecated, dbname should only exists in pg_db
      database->setDBName(db_catalog->GetDatabaseName());
      storage_manager->AddDatabaseToStorageManager(database);

      // put database object into rw_object_set
      txn->RecordCreate(db_oid, INVALID_OID, INVALID_OID);

      // add core & non-core system catalog tables into database
      catalog->BootstrapSystemCatalogs(database, txn);
      catalog->catalog_map_[db_oid]->Bootstrap(db_catalog->GetDatabaseName(),
      		txn);
    } else {
    	LOG_DEBUG("Use existed database storage object %d '%s'", db_oid,
                db_catalog->GetDatabaseName().c_str());
    	LOG_DEBUG("And use its system catalog objects");
    }

    for (auto table_catalog :
         db_catalog->GetTableObjects((std::string)CATALOG_SCHEMA_NAME)) {
      if (LoadCatalogTableCheckpoint(epoch_id, db_oid,
      		table_catalog->GetTableOid(), txn) == false) {
        txn_manager.AbortTransaction(txn);
      	return false;
      }
    }  // table loop end

  }  // database loop end

  txn_manager.CommitTransaction(txn);
  return true;
}


bool TimestampCheckpointManager::LoadCatalogTableCheckpoint(
		const eid_t &epoch_id, const oid_t db_oid, const oid_t table_oid,
		concurrency::TransactionContext *txn) {
	auto catalog = catalog::Catalog::GetInstance();
	auto db_catalog = catalog->GetDatabaseObject(db_oid, txn);
	auto table_catalog = db_catalog->GetTableObject(table_oid);
  auto table_name = table_catalog->GetTableName();
  auto system_catalogs = catalog->GetSystemCatalogs(db_oid);

  auto storage_manager = storage::StorageManager::GetInstance();
  auto table = storage_manager->GetTableWithOid(db_oid, table_oid);

  // load checkpoint files for catalog data.
  // except for system catalogs in catalog database to avoid error
  // when other catalog tables disappears by settings like
  // pg_query_history (brain settings).
  // also except for catalog requiring to initialize values:
  // LangageCatalog, ProcCatalog, SettingsCatalog.

	// catalogs out of recovery
	if (table_name == "pg_settings" || table_name == "pg_column_stats" ||
			table_name == "zone_map" || (db_oid == CATALOG_DATABASE_OID && (
					table_oid == SCHEMA_CATALOG_OID || table_oid == TABLE_CATALOG_OID ||
					table_oid == COLUMN_CATALOG_OID || table_oid == INDEX_CATALOG_OID ||
					table_oid == LAYOUT_CATALOG_OID))) {
		// nothing to do (keep the default values, and not recover other data)
	} else {
		// read a checkpoint file for the catalog
		oid_t oid_align;
		FileHandle table_file;
		std::string table_filename = GetCheckpointFileFullPath(
				db_catalog->GetDatabaseName(), table_catalog->GetSchemaName(),
				table_name, epoch_id);
		if (LoggingUtil::OpenFile(table_filename.c_str(), "rb", table_file) ==
				false) {
			LOG_ERROR("Checkpoint file for table %d '%s' is not existed",
								table_catalog->GetTableOid(), table_name.c_str());
			return false;
		}

		LOG_DEBUG("Recover catalog table %d '%s.%s' in database %d '%s'",
							table_catalog->GetTableOid(),
							table_catalog->GetSchemaName().c_str(), table_name.c_str(),
							db_catalog->GetDatabaseOid(),
							db_catalog->GetDatabaseName().c_str());

		// catalogs with duplicate check
		// keep the default values, but other data is recovered
		if (table_oid == DATABASE_CATALOG_OID || table_oid == SCHEMA_CATALOG_OID ||
				table_oid == TABLE_CATALOG_OID || table_oid == COLUMN_CATALOG_OID ||
				table_oid == INDEX_CATALOG_OID || table_oid == LAYOUT_CATALOG_OID ||
				table_name == "pg_language" || table_name == "pg_proc") {
			oid_align =
					RecoverTableDataWithDuplicateCheck(table, table_file, txn);
		}
		// catalogs to be recovered without duplicate check
		else {
			oid_align = RecoverTableDataWithoutTileGroup(table, table_file, txn);
		}

		LoggingUtil::CloseFile(table_file);

		// modify next OID of each catalog
		if (table_oid == DATABASE_CATALOG_OID) {
			catalog::DatabaseCatalog::GetInstance()->oid_ += oid_align;
		} else if (table_oid == SCHEMA_CATALOG_OID) {
			system_catalogs->GetSchemaCatalog()->oid_ += oid_align;
		} else if (table_oid == TABLE_CATALOG_OID) {
			system_catalogs->GetTableCatalog()->oid_ += oid_align;
		} else if (table_oid == COLUMN_CATALOG_OID) {
			// OID is not used
		} else if (table_oid == INDEX_CATALOG_OID) {
			system_catalogs->GetIndexCatalog()->oid_ += oid_align;
		} else if (table_oid == LAYOUT_CATALOG_OID) {
			// OID is not used
		} else if (table_name == "pg_proc") {
			catalog::ProcCatalog::GetInstance().oid_ += oid_align;
		} else if (table_name == "pg_trigger") {
			system_catalogs->GetTriggerCatalog()->oid_ += oid_align;
		} else if (table_name == "pg_language") {
			catalog::LanguageCatalog::GetInstance().oid_ += oid_align;
		} else if (table_name == "pg_database_metrics" ||
				table_name == "pg_table_metrics" || table_name == "pg_index_metrics" ||
				table_name == "pg_query_metrics" ||	table_name == "pg_query_history") {
			// OID is not used
		} else {
			LOG_ERROR("Unexpected catalog table appears: %s", table_name.c_str());
			PELOTON_ASSERT(false);
		}
	}

	return true;
}

bool TimestampCheckpointManager::LoadUserTableCheckpoint(const eid_t &epoch_id) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

	// Recover storage object
  FileHandle metadata_file;
  std::string metadata_filename = GetMetadataFileFullPath(epoch_id);
  if (LoggingUtil::OpenFile(metadata_filename.c_str(), "rb", metadata_file) ==
      false) {
  	txn_manager.AbortTransaction(txn);
    LOG_ERROR("Open checkpoint metadata file failed!");
    return false;
  }
  if (RecoverStorageObject(metadata_file, txn) == false) {
  	txn_manager.AbortTransaction(txn);
    LOG_ERROR("Storage object recovery failed");
    return false;
  }
  LoggingUtil::CloseFile(metadata_file);
  txn_manager.CommitTransaction(txn);

  // Recover table
  txn = txn_manager.BeginTransaction();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto catalog = catalog::Catalog::GetInstance();
  for (auto db_catalog_pair : catalog->GetDatabaseObjects(txn)) {
    auto db_oid = db_catalog_pair.first;
    auto database = storage_manager->GetDatabaseWithOid(db_oid);
    auto db_catalog = db_catalog_pair.second;

    // the catalog database has already been recovered.
    if (db_oid == CATALOG_DATABASE_OID) continue;

    for (auto table_catalog_pair : db_catalog->GetTableObjects()) {
      auto table_oid = table_catalog_pair.first;
      auto table = database->GetTableWithOid(table_oid);
      auto table_catalog = table_catalog_pair.second;

      // catalog tables in each database have already benn recovered
      if (table_catalog->GetSchemaName() == CATALOG_SCHEMA_NAME) continue;

      // read a checkpoint file for the catalog
      FileHandle table_file;
      std::string table_filename = GetCheckpointFileFullPath(
          db_catalog->GetDatabaseName(), table_catalog->GetSchemaName(),
          table_catalog->GetTableName(), epoch_id);
      if (LoggingUtil::OpenFile(table_filename.c_str(), "rb", table_file) ==
          false) {
        LOG_ERROR("Checkpoint file for table %d '%s' is not existed", table_oid,
                  table_catalog->GetTableName().c_str());
        continue;
      }

      LOG_DEBUG("Recover user table %d '%s.%s' in database %d '%s'", table_oid,
                table_catalog->GetSchemaName().c_str(),
                table_catalog->GetTableName().c_str(),
                db_catalog->GetDatabaseOid(),
                db_catalog->GetDatabaseName().c_str());

      // recover the table from the checkpoint file
      RecoverTableData(table, table_file, txn);

      LoggingUtil::CloseFile(table_file);

    }  // table loop end

  }  // database loop end

  txn_manager.CommitTransaction(txn);
  return true;
}


// TODO: Use data in catalog table to create storage objects (not serialized
// catalog object data)
bool TimestampCheckpointManager::RecoverStorageObject(
		FileHandle &file_handle, concurrency::TransactionContext *txn) {
  // read metadata file to recovered storage object
  size_t metadata_size = LoggingUtil::GetFileSize(file_handle);
  std::unique_ptr<char[]> metadata_data(new char[metadata_size]);

  LOG_DEBUG("Recover storage object (%lu byte)", metadata_size);

  if (LoggingUtil::ReadNBytesFromFile(file_handle, metadata_data.get(),
                                      metadata_size) == false) {
    LOG_ERROR("Checkpoint metadata file read error");
    return false;
  }

  CopySerializeInput metadata_buffer(metadata_data.get(), metadata_size);
  auto catalog = catalog::Catalog::GetInstance();
  auto storage_manager = storage::StorageManager::GetInstance();

  // recover database storage object
  size_t db_size = metadata_buffer.ReadLong();
  for (oid_t db_idx = 0; db_idx < db_size; db_idx++) {
    oid_t db_oid = metadata_buffer.ReadInt();
    auto db_catalog = catalog->GetDatabaseObject(db_oid, txn);
    PELOTON_ASSERT(db_catalog != nullptr);

    // database object has already been recovered in catalog recovery
    auto database = storage_manager->GetDatabaseWithOid(db_oid);

    LOG_DEBUG("Recover table object for database %d '%s'",
    		db_catalog->GetDatabaseOid(), db_catalog->GetDatabaseName().c_str());

    // recover table storage objects
    size_t table_size = metadata_buffer.ReadLong();
    for (oid_t table_idx = 0; table_idx < table_size; table_idx++) {
      oid_t table_oid = metadata_buffer.ReadInt();

      auto table_catalog = db_catalog->GetTableObject(table_oid);
      PELOTON_ASSERT(table_catalog != nullptr);

      LOG_DEBUG("Create table object %d '%s.%s'", table_oid,
                table_catalog->GetSchemaName().c_str(),
                table_catalog->GetTableName().c_str());

      // recover column information
      size_t column_count = metadata_buffer.ReadLong();
      std::vector<catalog::Column> columns;
      for (oid_t column_idx = 0; column_idx < column_count; column_idx++) {
        oid_t column_oid = metadata_buffer.ReadInt();
        size_t column_length = metadata_buffer.ReadLong();

        auto column_catalog = table_catalog->GetColumnObject(column_oid);
        PELOTON_ASSERT(column_catalog != nullptr);

        // create column storage object
        // ToDo: Column should be recovered from catalog
        auto column = catalog::Column(
            column_catalog->GetColumnType(), column_length,
            column_catalog->GetColumnName(), column_catalog->IsInlined(),
            column_catalog->GetColumnOffset());

        // recover column constraints
        // ToDo: Constraint should be recovered from catalog
        size_t column_constraint_count = metadata_buffer.ReadLong();
        for (oid_t constraint_idx = 0; constraint_idx < column_constraint_count;
             constraint_idx++) {
          auto column_constraint = catalog::Constraint::DeserializeFrom(
              metadata_buffer, column.GetType());
          // Foreign key constraint will be stored by DataTable deserializer
          if (column_constraint.GetType() != ConstraintType::FOREIGN) {
            column.AddConstraint(column_constraint);
          }
        }

        // Set a column into the vector in order of the column_oid.
        // Cannot use push_back of vector because column_catalog doesn't acquire
        // the column info from pg_attribute in the order.
        // If use []operator of vector to insert it, AddressSanitizer regards it
        // as stack-buffer-overflow in travis-ci (only release build).
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

      }  // column loop end

      std::unique_ptr<catalog::Schema> schema(new catalog::Schema(columns));

      // read schema information (multi-column constraints)
      size_t multi_constraint_count = metadata_buffer.ReadLong();
      for (oid_t multi_constraint_idx = 0;
           multi_constraint_idx < multi_constraint_count;
           multi_constraint_idx++) {
        schema->AddMultiConstraints(
            catalog::MultiConstraint::DeserializeFrom(metadata_buffer));
      }

      // create table storage object
      bool own_schema = true;
      bool adapt_table = false;
      bool is_catalog = false;
      storage::DataTable *table = storage::TableFactory::GetDataTable(
          db_oid, table_oid, schema.release(), table_catalog->GetTableName(),
          DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table, is_catalog);
      database->AddTable(table, is_catalog);

      // put data table object into rw_object_set
      txn->RecordCreate(db_oid, table_oid, INVALID_OID);

      // recover foreign key information as sink table
      auto foreign_key_count = metadata_buffer.ReadLong();
      for (oid_t fk_idx = 0; fk_idx < foreign_key_count; fk_idx++) {
        table->AddForeignKey(
            catalog::ForeignKey::DeserializeFrom(metadata_buffer));
      }

      // recover foreign key information as source table
      auto foreign_key_src_count = metadata_buffer.ReadLong();
      for (oid_t fk_src_idx = 0; fk_src_idx < foreign_key_src_count;
           fk_src_idx++) {
        table->RegisterForeignKeySource(
            catalog::ForeignKey::DeserializeFrom(metadata_buffer));
      }

      // recover trigger object of the storage table
      auto trigger_list =
          catalog->GetSystemCatalogs(db_oid)->GetTriggerCatalog()->GetTriggers(
              table_oid, txn);
      for (int trigger_idx = 0;
           trigger_idx < trigger_list->GetTriggerListSize(); trigger_idx++) {
        auto trigger = trigger_list->Get(trigger_idx);
        table->AddTrigger(*trigger);
      }

      // tuning

      // recover index storage objects
      auto index_catalogs = table_catalog->GetIndexObjects();
      for (auto index_catalog_pair : index_catalogs) {
        auto index_oid = index_catalog_pair.first;
        auto index_catalog = index_catalog_pair.second;

        LOG_TRACE(
            "|- Index %d '%s':  Index type %s, Index constraint %s, unique "
            "keys %d",
            index_oid, index_catalog->GetIndexName().c_str(),
            IndexTypeToString(index_catalog->GetIndexType()).c_str(),
            IndexConstraintTypeToString(index_catalog->GetIndexConstraint())
                .c_str(),
            index_catalog->HasUniqueKeys());

        auto key_attrs = index_catalog->GetKeyAttrs();
        auto key_schema =
            catalog::Schema::CopySchema(table->GetSchema(), key_attrs);
        key_schema->SetIndexedColumns(key_attrs);

        // Set index metadata
        auto index_metadata = new index::IndexMetadata(
            index_catalog->GetIndexName(), index_oid, table_oid, db_oid,
            index_catalog->GetIndexType(), index_catalog->GetIndexConstraint(),
            table->GetSchema(), key_schema, key_attrs,
            index_catalog->HasUniqueKeys());

        // create index storage objects and add it to the table
        std::shared_ptr<index::Index> key_index(
            index::IndexFactory::GetIndex(index_metadata));
        table->AddIndex(key_index);

        // Put index object into rw_object_set
        txn->RecordCreate(db_oid, table_oid, index_oid);

      }  // index loop end

    }  // table loop end

  }  // database loop end

  return true;
}

void TimestampCheckpointManager::RecoverTableData(
    storage::DataTable *table, FileHandle &file_handle,
    concurrency::TransactionContext *txn) {
  size_t table_size = LoggingUtil::GetFileSize(file_handle);
  if (table_size == 0) return;
  std::unique_ptr<char[]> data(new char[table_size]);
  if (LoggingUtil::ReadNBytesFromFile(file_handle, data.get(), table_size) ==
      false) {
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
  oid_t tile_group_count = input_buffer.ReadLong();
  for (oid_t tg_idx = START_OID; tg_idx < tile_group_count; tg_idx++) {
    // recover tile group
    std::shared_ptr<storage::TileGroup> tile_group(
    		storage::TileGroup::DeserializeFrom(input_buffer,
    				table->GetDatabaseOid(), table));

    LOG_TRACE("Deserialized tile group %u in %s \n%s", tile_group->GetTileGroupId(),
    		table->GetName().c_str(), tile_group->GetLayout().GetInfo().c_str());

    // add the tile group to table
    oid_t active_tile_group_id = tg_idx % table->GetActiveTileGroupCount();
    table->AddTileGroup(tile_group, active_tile_group_id);

    // recover tuples located in the tile group
    oid_t visible_tuple_count = input_buffer.ReadLong();
    oid_t column_count = schema->GetColumnCount();
    for (oid_t tuple_idx = 0; tuple_idx < visible_tuple_count; tuple_idx++) {
      // recover values on each column
      std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
      for (oid_t column_id = 0; column_id < column_count; column_id++) {
        auto value = type::Value::DeserializeFrom(
            input_buffer, schema->GetType(column_id), NULL);
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
        if (table->InsertTuple(tuple.get(), location, txn, &index_entry_ptr,
                               false) == true) {
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
    storage::DataTable *table, FileHandle &file_handle,
    concurrency::TransactionContext *txn) {
  size_t table_size = LoggingUtil::GetFileSize(file_handle);
  if (table_size == 0) return 0;
  std::unique_ptr<char[]> data(new char[table_size]);
  if (LoggingUtil::ReadNBytesFromFile(file_handle, data.get(), table_size) ==
      false) {
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
      auto value = type::Value::DeserializeFrom(
          input_buffer, schema->GetType(column_id), NULL);
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
    storage::DataTable *table, FileHandle &file_handle,
    concurrency::TransactionContext *txn) {
  size_t table_size = LoggingUtil::GetFileSize(file_handle);
  if (table_size == 0) return 0;
  std::unique_ptr<char[]> data(new char[table_size]);
  if (LoggingUtil::ReadNBytesFromFile(file_handle, data.get(), table_size) ==
      false) {
    LOG_ERROR("Checkpoint table file read error");
    return 0;
  }
  CopySerializeInput input_buffer(data.get(), table_size);

  LOG_TRACE("Recover table %d data with duplicate check (%lu byte)",
            table->GetOid(), table_size);

  // look for all primary key columns
  std::vector<oid_t> pk_columns;
  auto schema = table->GetSchema();
  oid_t column_count = schema->GetColumnCount();
  for (oid_t column_id = 0; column_id < column_count; column_id++) {
    if (schema->GetColumn(column_id).IsPrimary()) {
      pk_columns.push_back(column_id);
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
      auto value = type::Value::DeserializeFrom(
          input_buffer, schema->GetType(column_id), NULL);
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
                  .CompareNotEquals(tuple->GetValue(pk_column)) ==
              CmpBool::CmpTrue) {
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

}  // namespace logging
}  // namespace peloton
