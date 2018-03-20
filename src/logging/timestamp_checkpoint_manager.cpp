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
#include "catalog/table_catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/schema.h"
#include "catalog/column.h"
#include "common/timer.h"
#include "concurrency/transaction_manager_factory.h"
#include "concurrency/timestamp_ordering_transaction_manager.h"
#include "storage/storage_manager.h"
#include "storage/database.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/tile_group_header.h"
#include "type/serializeio.h"
#include "type/types.h"


namespace peloton {
namespace logging {

void TimestampCheckpointManager::StartCheckpointing() {
	is_running_ = true;
	central_checkpoint_thread_.reset(new std::thread(&TimestampCheckpointManager::Running, this));
}

void TimestampCheckpointManager::StopCheckpointing() {
	is_running_ = false;
	central_checkpoint_thread_->join();
}

bool TimestampCheckpointManager::DoRecovery(){
	eid_t epoch_id = GetRecoveryCheckpointEpoch();
	if (epoch_id == INVALID_EID) {
		LOG_INFO("No checkpoint for recovery");
		return false;
	} else {
		LOG_INFO("Do checkpoint recovery");
		Timer<std::milli> recovery_timer;
		recovery_timer.Start();

		if(PerformCheckpointRecovery(epoch_id) == false) {
			LOG_INFO("Checkpoint recovery was failed");
			return false;
		}

		recovery_timer.Stop();
		LOG_INFO("Checkpoint recovery time: %lf ms", recovery_timer.GetDuration());
	}
	return true;
}


void TimestampCheckpointManager::Running() {
	int count = checkpoint_interval_ - 1;
	while(1) {
		if (is_running_ == false) {
			LOG_INFO("Finish checkpoint thread");
			break;
		}
		// wait for interval
		std::this_thread::sleep_for(std::chrono::seconds(1));
		count++;
		if(count == checkpoint_interval_) {
			LOG_INFO("Do checkpointing");
			Timer<std::milli> checkpoint_timer;
			checkpoint_timer.Start();

			count = 0;
			PerformCheckpointing();

			checkpoint_timer.Stop();
			LOG_INFO("Checkpointing time: %lf ms", checkpoint_timer.GetDuration());
		}
	}
}

void TimestampCheckpointManager::PerformCheckpointing() {
	// begin transaction and get epoch id as this checkpointing beginning
	auto &txn_manager = concurrency::TimestampOrderingTransactionManager::GetInstance(
				ProtocolType::TIMESTAMP_ORDERING,
				IsolationLevelType::SERIALIZABLE,
				ConflictAvoidanceType::WAIT
			);
	auto txn = txn_manager.BeginTransaction();
	cid_t begin_cid = txn->GetReadId();
	eid_t begin_epoch_id = txn->GetEpochId();

	// create checkpoint directory
	CreateWorkingCheckpointDirectory();

	// prepare for data loading
	auto catalog = catalog::Catalog::GetInstance();
	auto storage_manager = storage::StorageManager::GetInstance();
	auto db_count = storage_manager->GetDatabaseCount();
	std::vector<std::shared_ptr<catalog::DatabaseCatalogObject>> target_db_catalogs;
	std::vector<std::shared_ptr<catalog::TableCatalogObject>> target_table_catalogs;

	// do checkpointing to take tables into each file
	for (oid_t db_idx = START_OID; db_idx < db_count; db_idx++) {
		try {
			auto database = storage_manager->GetDatabaseWithOffset(db_idx);
			auto db_catalog = catalog->GetDatabaseObject(database->GetOid(), txn);

			// make sure the database exists in this epoch.
			// catalog database is out of checkpoint.
			if (db_catalog != nullptr && db_catalog->GetDatabaseOid() != CATALOG_DATABASE_OID) {
				auto table_count = database->GetTableCount();

				// collect database info for catalog file
				target_db_catalogs.push_back(db_catalog);

				for (oid_t table_idx = START_OID; table_idx < table_count; table_idx++) {
					try {
						auto table = database->GetTable(table_idx);
						auto table_catalog = db_catalog->GetTableObject(table->GetOid());

						// make sure the table exists in this epoch
						if (table_catalog != nullptr) {
							std::string filename = GetWorkingCheckpointFileFullPath(database->GetDBName(), table->GetName());
							FileHandle table_file;

							// make a table file
							if(LoggingUtil::OpenFile(filename.c_str(), "wb", table_file) != true) {
								txn_manager.EndTransaction(txn);
								LOG_ERROR("Create checkpoint file failed!");
								return;
							}
							CheckpointingTable(table, begin_cid, table_file);
							fclose(table_file.file);

							// collect table info for catalog file
							target_table_catalogs.push_back(table_catalog);

						} else {
							LOG_DEBUG("Table %d in database %s (%d) is invisible.",
									table->GetOid(), db_catalog->GetDatabaseName().c_str(), database->GetOid());
						}
					} catch (CatalogException& e) {
						LOG_DEBUG("%s", e.what());
					}

				} // end table loop

			} else {
				LOG_DEBUG("Database %s (%d) is invisible or catalog database.", database->GetDBName().c_str(), database->GetOid());
			}
		} catch (CatalogException& e) {
			LOG_DEBUG("%s", e.what());
		}
	} // end database loop

	// do checkpointing to catalog data
	FileHandle catalog_file;
	std::string catalog_filename = GetWorkingCatalogFileFullPath();
	if (LoggingUtil::OpenFile(catalog_filename.c_str(), "wb", catalog_file) != true) {
		LOG_ERROR("Create catalog file failed!");
		txn_manager.EndTransaction(txn);
		return;
	}
	CheckpointingCatalog(target_db_catalogs, target_table_catalogs, catalog_file);
	fclose(catalog_file.file);

	// end transaction
	txn_manager.EndTransaction(txn);

	LOG_INFO("Complete Checkpointing in epoch %lu (cid = %lu)",
			concurrency::EpochManagerFactory::GetInstance().GetCurrentEpochId(), begin_cid);

	// finalize checkpoint directory
	MoveWorkingToCheckpointDirectory(std::to_string(begin_epoch_id));
	RemoveOldCheckpoints(begin_epoch_id);
}

void TimestampCheckpointManager::CheckpointingTable(const storage::DataTable *target_table, const cid_t &begin_cid, FileHandle &file_handle) {
	CopySerializeOutput output_buffer;

	LOG_DEBUG("Done checkpointing to table %d '%s' in database %d",
			target_table->GetOid(), target_table->GetName().c_str(), target_table->GetDatabaseOid());

	// load all table data
	oid_t tile_group_count = target_table->GetTileGroupCount();
	for (oid_t tile_group_offset = START_OID; tile_group_offset < tile_group_count; tile_group_offset++) {
		auto tile_group = target_table->GetTileGroup(tile_group_offset);
		auto tile_group_header = tile_group->GetHeader();

		// load all tuple data in the table
		oid_t tuple_count = tile_group->GetNextTupleSlot();
		for (oid_t tuple_id = START_OID; tuple_id < tuple_count; tuple_id++) {
			if (IsVisible(tile_group_header, tuple_id, begin_cid)) {
				// load all field data of each column in the tuple
				oid_t column_count = tile_group->GetColumnMap().size();
				for (oid_t column_id = START_OID; column_id < column_count; column_id++){
					type::Value value = tile_group->GetValue(tuple_id, column_id);
					value.SerializeTo(output_buffer);
					LOG_TRACE("%s(column %d, tuple %d):%s\n",
							target_table->GetName().c_str(), column_id, tuple_id, value.ToString().c_str());
				}

				int ret = fwrite((void *)output_buffer.Data(), output_buffer.Size(), 1, file_handle.file);
				if (ret != 1) {
					LOG_ERROR("Write error (tuple %d, table %d)", tuple_id, target_table->GetOid());
					return;
				}

				output_buffer.Reset();
			} else {
				LOG_TRACE("%s's tuple %d is invisible\n", target_table->GetName().c_str(), tuple_id);
			}
		}

	}

	LoggingUtil::FFlushFsync(file_handle);
}

bool TimestampCheckpointManager::IsVisible(const storage::TileGroupHeader *header, const oid_t &tuple_id, const cid_t &begin_cid) {
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
		if (activated && !invalidated) {
			return true;
		} else {
			return false;
		}
	}	else {
		// this tuple is owned by othre transactions.
		if (tuple_begin_cid == MAX_CID) {
			// this tuple is an uncommitted version.
			return false;
		} else {
			if (activated && !invalidated) {
				return true;
			} else {
				return false;
			}
		}
	}
}

// TODO: migrate below serializing processes into each class file
//			(DatabaseCatalogObject, TableCatalogObject, Index)
void TimestampCheckpointManager::CheckpointingCatalog(
		const std::vector<std::shared_ptr<catalog::DatabaseCatalogObject>> &target_db_catalogs,
		const std::vector<std::shared_ptr<catalog::TableCatalogObject>> &target_table_catalogs,
		FileHandle &file_handle) {
	CopySerializeOutput catalog_buffer;
	auto storage_manager = storage::StorageManager::GetInstance();

	// insert # of databases into catalog file
	size_t db_count = target_db_catalogs.size();
	catalog_buffer.WriteLong(db_count);

	// insert each database information into catalog file
	for (auto db_catalog : target_db_catalogs) {
		auto database = storage_manager->GetDatabaseWithOid(db_catalog->GetDatabaseOid());

		// write database information (ID, name, and table count)
		catalog_buffer.WriteInt(db_catalog->GetDatabaseOid());
		catalog_buffer.WriteTextString(db_catalog->GetDatabaseName());
		catalog_buffer.WriteLong(target_table_catalogs.size());

		LOG_DEBUG("Write database catalog %d '%s' : %lu tables",
				db_catalog->GetDatabaseOid(), db_catalog->GetDatabaseName().c_str(), target_table_catalogs.size());

		// insert each table information into catalog file
		for (auto table_catalog : target_table_catalogs) {
			auto table = database->GetTableWithOid(table_catalog->GetTableOid());

			// Write table information (ID, name and size)
			catalog_buffer.WriteInt(table_catalog->GetTableOid());
			catalog_buffer.WriteTextString(table_catalog->GetTableName());

			// Write schema information (# of columns)
			auto schema = table->GetSchema();

			schema->SerializeTo(catalog_buffer);
			LOG_DEBUG("Write table catalog %d '%s': %lu columns",
					table_catalog->GetTableOid(), table_catalog->GetTableName().c_str(), schema->GetColumnCount());

			// Write index information (index name, type, constraint, key attributes, and # of index)
			auto index_catalogs = table_catalog->GetIndexObjects();
			catalog_buffer.WriteLong(index_catalogs.size());
			for (auto index_catalog_pair : index_catalogs) {
				// Index basic information
				auto index_catalog = index_catalog_pair.second;
				catalog_buffer.WriteTextString(index_catalog->GetIndexName());
				catalog_buffer.WriteInt((int)index_catalog->GetIndexType());
				catalog_buffer.WriteInt((int)index_catalog->GetIndexConstraint());
				catalog_buffer.WriteBool(index_catalog->HasUniqueKeys());
				LOG_DEBUG("|- Index '%s':  Index type %s, Index constraint %s, unique keys %d",
						index_catalog->GetIndexName().c_str(), IndexTypeToString(index_catalog->GetIndexType()).c_str(),
						IndexConstraintTypeToString(index_catalog->GetIndexConstraint()).c_str(), index_catalog->HasUniqueKeys());

				// Index key attributes
				auto key_attrs = index_catalog->GetKeyAttrs();
				catalog_buffer.WriteLong(key_attrs.size());
				for (auto attr_oid : key_attrs) {
					catalog_buffer.WriteInt(attr_oid);
					LOG_DEBUG("|  |- Key attribute %d '%s'", attr_oid, table_catalog->GetColumnObject(attr_oid)->GetColumnName().c_str());
				}
			} // end index loop

		} // end table loop

	} // end database loop

	// Output data to file
	int ret = fwrite((void *)catalog_buffer.Data(), catalog_buffer.Size(), 1, file_handle.file);
	if (ret != 1) {
		LOG_ERROR("Write error");
		return;
	}
	LoggingUtil::FFlushFsync(file_handle);
}

bool TimestampCheckpointManager::PerformCheckpointRecovery(const eid_t &epoch_id) {
	// begin a transaction to recover tuples into each table.
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	auto txn = txn_manager.BeginTransaction();

	// Recover catalog
	FileHandle catalog_file;
	std::string catalog_filename = GetCatalogFileFullPath(epoch_id);
	if (LoggingUtil::OpenFile(catalog_filename.c_str(), "rb", catalog_file) != true) {
		txn_manager.AbortTransaction(txn);
		LOG_ERROR("Create checkpoint file failed!");
		return false;
	}
	RecoverCatalog(catalog_file, txn);
	fclose(catalog_file.file);

	// Recover table
	auto storage_manager = storage::StorageManager::GetInstance();
	auto db_count = storage_manager->GetDatabaseCount();
	for (oid_t db_idx = START_OID; db_idx < db_count; db_idx++) {
		auto database = storage_manager->GetDatabaseWithOffset(db_idx);

		// the recovery targets databases except for catalog database.
		if (database->GetOid() != CATALOG_DATABASE_OID) {
			auto table_count = database->GetTableCount();

			for (oid_t table_idx = START_OID; table_idx < table_count; table_idx++) {
				FileHandle table_file;
				auto table = database->GetTable(table_idx);
				std::string table_filename = GetCheckpointFileFullPath(database->GetDBName(), table->GetName(), epoch_id);
				if (LoggingUtil::OpenFile(table_filename.c_str(), "rb", table_file) != true) {
					txn_manager.AbortTransaction(txn);
					LOG_ERROR("Open checkpoint file %s failed!", table_filename.c_str());
					return false;
				}
				RecoverTable(table, table_file, txn);
				fclose(table_file.file);
			}

		} else {
			LOG_DEBUG("Database %s (%d) is invisible or catalog database.", database->GetDBName().c_str(), database->GetOid());
		}

	}

	txn_manager.CommitTransaction(txn);
	LOG_INFO("Complete checkpoint recovery in epoch %lu", epoch_id);
	return true;
}

// TODO: migrate below deserializing processes into each class file
//			(DatabaseCatalogObject, TableCatalogObject, Index)
void TimestampCheckpointManager::RecoverCatalog(FileHandle &file_handle, concurrency::Transaction *txn) {
	// read catalog file to be recovered
	size_t catalog_size = LoggingUtil::GetFileSize(file_handle);
	char catalog_data[catalog_size];

	LOG_DEBUG("Recover catalog data (%lu byte)", catalog_size);

	if (LoggingUtil::ReadNBytesFromFile(file_handle, catalog_data, catalog_size) == false) {
		LOG_ERROR("checkpoint catalog file read error");
		return;
	}

	CopySerializeInput catalog_buffer(catalog_data, catalog_size);
	auto catalog = catalog::Catalog::GetInstance();

	// recover database catalog
	size_t db_count = catalog_buffer.ReadLong();
	for(oid_t db_idx = 0; db_idx < db_count; db_idx++) {
		// read basic database information
		oid_t db_oid = catalog_buffer.ReadInt();
		std::string db_name = catalog_buffer.ReadTextString();
		size_t table_count = catalog_buffer.ReadLong();

		// create database catalog
		// if already exists, use existed database
		if (catalog->ExistsDatabaseByName(db_name, txn) == false) {
			LOG_DEBUG("Create database %d '%s' (including %lu tables)", db_oid, db_name.c_str(), table_count);
			auto result = catalog->CreateDatabase(db_name, txn);
			if (result != ResultType::SUCCESS) {
				LOG_ERROR("Create database error");
				return;
			}
		} else {
			LOG_DEBUG("Use existing database %d '%s'", db_oid, db_name.c_str());
		}
		oid_t new_db_oid = catalog->GetDatabaseObject(db_name, txn)->GetDatabaseOid();

		// Recover table catalog
		for (oid_t table_idx = 0; table_idx < table_count; table_idx++) {
			// read basic table information
			oid_t table_oid = catalog_buffer.ReadInt();
			std::string table_name = catalog_buffer.ReadTextString();
			LOG_DEBUG("Create table %d '%s'", table_oid, table_name.c_str());

			// recover table schema
			std::unique_ptr<catalog::Schema> schema = catalog::Schema::DeserializeFrom(catalog_buffer);

			// create table
			// if already exists, abort the catalog recovery
			if (catalog->ExistsTableByName(db_name, table_name, txn) == false) {
				catalog->CreateTable(db_name, table_name, std::move(schema), txn);
			} else {
				LOG_DEBUG("%s table already exists", table_name.c_str());
			}
			oid_t new_table_oid = catalog->GetTableObject(db_name, table_name, txn)->GetTableOid();

			// recover index catalog
			size_t index_count = catalog_buffer.ReadLong();
			for (oid_t index_idx = 0; index_idx < index_count; index_idx++) {
				// Index basic information
				std::string index_name = catalog_buffer.ReadTextString();
				IndexType index_type = (IndexType)catalog_buffer.ReadInt();
				IndexConstraintType index_constraint_type = (IndexConstraintType)catalog_buffer.ReadInt();
				bool index_unique_keys = catalog_buffer.ReadBool();
				LOG_DEBUG("|- Index '%s':  Index type %s, Index constraint %s, unique keys %d",
						index_name.c_str(), IndexTypeToString(index_type).c_str(),
						IndexConstraintTypeToString(index_constraint_type).c_str(), index_unique_keys);

				// Index key attributes
				std::vector<oid_t> key_attrs;
				size_t index_attr_count = catalog_buffer.ReadLong();
				for (oid_t index_attr_idx = 0; index_attr_idx < index_attr_count; index_attr_idx++) {
					oid_t index_attr = catalog_buffer.ReadInt();
					key_attrs.push_back(index_attr);
					LOG_DEBUG("|  |- Key attribute %d",	index_attr);
				}

				// create index if not primary key
				if (catalog->ExistsIndexByName(db_name, table_name, index_name, txn) == false) {
					catalog->CreateIndex(new_db_oid, new_table_oid, key_attrs, index_name, index_type, index_constraint_type, index_unique_keys, txn);
				} else {
					LOG_DEBUG("|  %s index already exists", index_name.c_str());
				}

			} // end index loop

		} // end table loop

	} // end database loop
}

void TimestampCheckpointManager::RecoverTable(storage::DataTable *table, FileHandle &file_handle, concurrency::Transaction *txn) {
	size_t table_size = LoggingUtil::GetFileSize(file_handle);
	char data[table_size];

	LOG_DEBUG("Recover table %s (%d) data (%lu byte)", table->GetName().c_str(), table->GetOid(), table_size);

	if (LoggingUtil::ReadNBytesFromFile(file_handle, data, table_size) == false) {
		LOG_ERROR("Checkpoint table file read error");
		return;
	}
	CopySerializeInput input_buffer(data, sizeof(data));

	auto schema = table->GetSchema();
	std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));

	/*
	auto tile_group_count = table->GetTileGroupCount();
	for (size_t tile_group_idx = 0; tile_group_idx < tile_group_count; tile_group_idx++) {
		auto tile_group = table->GetTileGroup(tile_group_idx);
		LOG_DEBUG("%s", tile_group->GetInfo().c_str());
	}
	*/

	// recover table tuples
	oid_t column_count = schema->GetColumnCount();
	while (input_buffer.RestSize() > 0) {
    // recover values on each column
    ItemPointer *index_entry_ptr = nullptr;
		for (oid_t column_id = 0; column_id < column_count; column_id++) {
			auto value = type::Value::DeserializeFrom(input_buffer, schema->GetType(column_id), NULL);
			tuple->SetValue(column_id, value);
		}

		// insert the deserialized tuple into the table
		ItemPointer location = table->InsertTuple(tuple.get(), txn, &index_entry_ptr);
		if (location.block == INVALID_OID) {
			LOG_ERROR("Tuple insert error");
			return;
		}
		concurrency::TransactionManagerFactory::GetInstance().PerformInsert(txn, location, index_entry_ptr);
	}

}

}  // namespace logging
}  // namespace peloton
