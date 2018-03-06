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
#include "catalog/constraint.h"
#include "catalog/multi_constraint.h"
#include "common/timer.h"
#include "concurrency/epoch_manager_factory.h"
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

void TimestampCheckpointManager::DoRecovery(){
	eid_t epoch_id = GetRecoveryCheckpointEpoch();
	if (epoch_id == 0) {
		LOG_INFO("No checkpoint for recovery");
	} else {
		LOG_INFO("Do checkpoint recovery");
		Timer<std::milli> checkpoint_timer;
		checkpoint_timer.Start();

		PerformCheckpointRecovery(epoch_id);

		checkpoint_timer.Stop();
		LOG_INFO("Checkpointing time: %lf ms", checkpoint_timer.GetDuration());
	}
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
	/*
	auto &epoch_manager = concurrency::EpochManagerFactory::GetInstance();
	// size_t thread_id = std::thread::hardware_concurrency() + 3; // Max thread id + 1 in thread pool
	size_t thread_id = 0;
	// cid_t begin_cid = epoch_manager.EnterEpoch(thread_id, TimestampType::SNAPSHOT_READ);
	cid_t begin_cid = epoch_manager.EnterEpoch(thread_id, TimestampType::READ);
	eid_t begin_epoch_id = begin_cid >> 32;
	*/
	auto &txn_manager = concurrency::TimestampOrderingTransactionManager::GetInstance(
				ProtocolType::TIMESTAMP_ORDERING,
//				IsolationLevelType::SNAPSHOT,
				IsolationLevelType::SERIALIZABLE,
				ConflictAvoidanceType::WAIT
			);
	auto txn = txn_manager.BeginTransaction();
	cid_t begin_cid = txn->GetReadId();
	eid_t begin_epoch_id = txn->GetEpochId();

	LOG_INFO("Start checkpointing in epoch %lu (cid = %lu)", begin_epoch_id, begin_cid);

	// create checkpoint directory
	CreateWorkingCheckpointDirectory();

	// open catalog file
	FileHandle catalog_file;
	std::string catalog_filename = GetWorkingCatalogFileFullPath();
	bool success = LoggingUtil::OpenFile(catalog_filename.c_str(), "wb", catalog_file);
	PL_ASSERT(success == true);
	if(success != true) {
		LOG_ERROR("Create catalog file failed!");
		return;
	}

	// prepare for data loading
	auto catalog = catalog::Catalog::GetInstance();
	auto storage_manager = storage::StorageManager::GetInstance();
	auto db_count = storage_manager->GetDatabaseCount();

	// insert database info (# of databases) into catalog file
	CheckpointingDatabaseCount(db_count, catalog_file);

	// do checkpointing to take tables into each file and make catalog files
	for (oid_t db_idx = START_OID; db_idx < db_count; db_idx++) {
		try {
			auto database = storage_manager->GetDatabaseWithOffset(db_idx);
			auto db_catalog = catalog->GetDatabaseObject(database->GetOid(), txn);

			// make sure the database exists in this epoch.
			// catalog database is out of checkpoint.
			if (db_catalog != nullptr && db_catalog->GetDatabaseOid() != CATALOG_DATABASE_OID) {
				auto table_count = database->GetTableCount();

				// insert database info (database name and table count) into catalog file
				CheckpointingDatabaseCatalog(db_catalog.get(), table_count, catalog_file);

				for (oid_t table_idx = START_OID; table_idx < table_count; table_idx++) {
					try {
						auto table_catalog = db_catalog->GetTableObject(database->GetTable(table_idx)->GetOid());

						// make sure the table exists in this epoch
						if (table_catalog != nullptr) {
							std::string filename = GetWorkingCheckpointFileFullPath(db_idx, table_idx);
							FileHandle table_file;
							auto table = database->GetTable(table_idx);
							auto table_catalog = db_catalog->GetTableObject(table->GetOid());

							// make a table file
							bool success = LoggingUtil::OpenFile(filename.c_str(), "wb", table_file);
							PL_ASSERT(success == true);
							if(success != true) {
								LOG_ERROR("Create checkpoint file failed!");
								return;
							}
							size_t table_size = CheckpointingTable(table, begin_cid, table_file);
							LOG_DEBUG("Done checkpointing to table %d '%s' (%lu byte) in database %d", table_idx, table->GetName().c_str(), table_size, db_idx);
							fclose(table_file.file);

							// insert table info into catalog file
							CheckpointingTableCatalog(table_catalog.get(), table->GetSchema(), table_size, catalog_file);

						} else {
							LOG_DEBUG("Table %d in database %s (%d) is invisible.", table_idx, db_catalog->GetDatabaseName().c_str(), db_idx);
						}
					} catch (CatalogException& e) {
						LOG_DEBUG("%s", e.what());
					}
				} // end table loop

			} else {
				LOG_DEBUG("Database %d is invisible.", db_idx);
			}
		} catch (CatalogException& e) {
			LOG_DEBUG("%s", e.what());
		}
	} // end database loop

	fclose(catalog_file.file);

	// end transaction
	//epoch_manager.ExitEpoch(thread_id, begin_epoch_id);
	txn_manager.EndTransaction(txn);

	LOG_INFO("Complete Checkpointing in epoch %lu (cid = %lu)", concurrency::EpochManagerFactory::GetInstance().GetCurrentEpochId(), begin_cid);

	// finalize checkpoint directory
	MoveWorkingToCheckpointDirectory(std::to_string(begin_epoch_id));
	RemoveOldCheckpoints(begin_epoch_id);
}

size_t TimestampCheckpointManager::CheckpointingTable(const storage::DataTable *target_table, const cid_t &begin_cid, FileHandle &file_handle) {
	CopySerializeOutput output_buffer;
	size_t table_size = 0;

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
					LOG_TRACE("%s(column %d, tuple %d):%s\n", target_table->GetName().c_str(), column_id, tuple_id, value.ToString().c_str());
				}

				int ret = fwrite((void *)output_buffer.Data(), output_buffer.Size(), 1, file_handle.file);
				if (ret != 1) {
					LOG_ERROR("Write error (tuple %d, table %d)", tuple_id, target_table->GetOid());
					return -1;
				}

				table_size += output_buffer.Size();
				output_buffer.Reset();
			} else {
				LOG_DEBUG("%s's tuple %d is invisible\n", target_table->GetName().c_str(), tuple_id);
			}
		}
	}

	LoggingUtil::FFlushFsync(file_handle);
	return table_size;
}

void TimestampCheckpointManager::CheckpointingDatabaseCount(const oid_t db_count, FileHandle &file_handle) {
	CopySerializeOutput catalog_buffer;

	catalog_buffer.WriteInt(db_count);

	int ret = fwrite((void *)catalog_buffer.Data(), catalog_buffer.Size(), 1, file_handle.file);
	if (ret != 1) {
		LOG_ERROR("Write error: database count %d", db_count);
		return;
	}

	LoggingUtil::FFlushFsync(file_handle);
}

void TimestampCheckpointManager::CheckpointingDatabaseCatalog(catalog::DatabaseCatalogObject *db_catalog, const oid_t table_count, FileHandle &file_handle) {
	CopySerializeOutput catalog_buffer;

	catalog_buffer.WriteTextString(db_catalog->GetDatabaseName());
	catalog_buffer.WriteInt(table_count);

	int ret = fwrite((void *)catalog_buffer.Data(), catalog_buffer.Size(), 1, file_handle.file);
	if (ret != 1) {
		LOG_ERROR("Write error (database '%s' catalog data)", db_catalog->GetDatabaseName().c_str());
		return;
	}

	LoggingUtil::FFlushFsync(file_handle);
}

// TODO: migrate below processes of serializations into each class file (TableCatalogObject, Schema, Column, MultiConstraint, Constraint, Index)
void TimestampCheckpointManager::CheckpointingTableCatalog(catalog::TableCatalogObject *table_catalog, catalog::Schema *schema, const size_t table_size, FileHandle &file_handle) {
	CopySerializeOutput catalog_buffer;

	// Write table information (ID, name and size)
	catalog_buffer.WriteTextString(table_catalog->GetTableName());
	catalog_buffer.WriteLong(table_size);

	// Write schema information (# of columns)
	catalog_buffer.WriteLong(schema->GetColumnCount());

	// Write schema information (multi-column constraints)
	auto multi_constraints = schema->GetMultiConstraints();
	catalog_buffer.WriteLong(multi_constraints.size());
	for (auto multi_constraint : multi_constraints) {
		auto constraint_columns = multi_constraint.GetCols();
		catalog_buffer.WriteLong(constraint_columns.size());
		for (auto column : constraint_columns) {
			catalog_buffer.WriteLong(column);
		}
		catalog_buffer.WriteTextString(multi_constraint.GetName());
		catalog_buffer.WriteInt((int)multi_constraint.GetType());
	}

	LOG_DEBUG("Write talbe catalog '%s' (%lu bytes): %lu columns",
			table_catalog->GetTableName().c_str(), table_size, schema->GetColumnCount());

	// Write each column information (column name, length, offset, type and constraints)
	for(auto column : schema->GetColumns()) {
		// Column basic information
		catalog_buffer.WriteTextString(column.GetName());
		catalog_buffer.WriteInt((int)column.GetType());
		catalog_buffer.WriteLong(column.GetLength());
		catalog_buffer.WriteLong(column.GetOffset());
		catalog_buffer.WriteBool(column.IsInlined());
		LOG_DEBUG("|- Column '%s %s': length %lu, offset %d, Inline %d",
				column.GetName().c_str(), TypeIdToString(column.GetType()).c_str(),
				column.GetLength(), column.GetOffset(), column.IsInlined());

		// Column constraints
		auto column_constraints = column.GetConstraints();
		catalog_buffer.WriteLong(column_constraints.size());
		for (auto column_constraint : column_constraints) {
			catalog_buffer.WriteTextString(column_constraint.GetName());
			catalog_buffer.WriteInt((int)column_constraint.GetType());
			catalog_buffer.WriteLong(column_constraint.GetForeignKeyListOffset());
			catalog_buffer.WriteLong(column_constraint.GetUniqueIndexOffset());
			LOG_DEBUG("|  |- Column constraint '%s %s': Foreign key list offset %d, Unique index offset %d",
					column_constraint.GetName().c_str(), ConstraintTypeToString(column_constraint.GetType()).c_str(),
					column_constraint.GetForeignKeyListOffset(), column_constraint.GetUniqueIndexOffset());

			if (column_constraint.GetType() == ConstraintType::DEFAULT) {
				auto default_value = column_constraint.getDefaultValue();
				default_value->SerializeTo(catalog_buffer);
				LOG_DEBUG("|  |      Default value %s", default_value->ToString().c_str());
			}

			if (column_constraint.GetType() == ConstraintType::CHECK) {
				auto exp = column_constraint.GetCheckExpression();
				catalog_buffer.WriteInt((int)exp.first);
				auto exp_value = exp.second;
				exp_value.SerializeTo(catalog_buffer);
				LOG_DEBUG("|  |      Check expression %s %s", ExpressionTypeToString(exp.first).c_str(), exp_value.ToString().c_str());
			}
		}
	}

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
		for (auto attr : key_attrs) {
			catalog_buffer.WriteLong(attr);
			LOG_DEBUG("|  |- Key attribute %d %s", attr, table_catalog->GetColumnObject(attr)->GetColumnName().c_str());
		}
	}

	// Output data to file
	int ret = fwrite((void *)catalog_buffer.Data(), catalog_buffer.Size(), 1, file_handle.file);
	if (ret != 1) {
		LOG_ERROR("Write error (table '%s' catalog data)", table_catalog->GetTableName().c_str());
		return;
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

void TimestampCheckpointManager::PerformCheckpointRecovery(const eid_t &epoch_id) {
	// Recover catalog
	FileHandle catalog_file;
	std::string catalog_filename = GetCatalogFileFullPath(epoch_id);
	bool success = LoggingUtil::OpenFile(catalog_filename.c_str(), "rb", catalog_file);
	PL_ASSERT(success == true);
	if(success != true) {
		LOG_ERROR("Create checkpoint file failed!");
		return;
	}
//	RecoverCatalog(catalog_file);

	// Recover table
	FileHandle table_file;
	std::string table_filename = GetCheckpointFileFullPath(1,0, epoch_id);
	success = LoggingUtil::OpenFile(table_filename.c_str(), "rb", table_file);
	PL_ASSERT(success == true);
	if(success != true) {
		LOG_ERROR("Create checkpoint file failed!");
		return;
	}
	RecoverTable(table_file);
	fclose(table_file.file);
}

void TimestampCheckpointManager::RecoverCatalog(FileHandle &file_handle) {
	size_t catalog_size = LoggingUtil::GetFileSize(file_handle);
	char catalog_data[catalog_size];

	LOG_DEBUG("Recover catalog data (%lu byte)", catalog_size);

	if (LoggingUtil::ReadNBytesFromFile(file_handle, catalog_data, catalog_size) == false) {
		LOG_ERROR("Read error");
		return;
	}

	CopySerializeInput catalog_buffer(catalog_data, catalog_size);

	// Recover database catalog
	std::string db_name = catalog_buffer.ReadTextString();
	size_t db_count = catalog_buffer.ReadLong();

	LOG_DEBUG("Read %lu database catalog", db_count);

		// Recover table catalog

		// Recover schema catalog

		// Recover column catalog

		// Recover index catalog

}

void TimestampCheckpointManager::RecoverTable(FileHandle &file_handle) {
	size_t table_size = LoggingUtil::GetFileSize(file_handle);
	char data[table_size];

	LOG_DEBUG("Recover table data (%lu byte)", table_size);

	if (LoggingUtil::ReadNBytesFromFile(file_handle, data, table_size) == false) {
		LOG_ERROR("Read error");
		return;
	}
	CopySerializeInput input_buffer(data, sizeof(data));
	for(int i=0; i<4; i++) {
		type::Value value1 = type::Value::DeserializeFrom(input_buffer, type::TypeId::INTEGER, NULL);
		type::Value value2 = type::Value::DeserializeFrom(input_buffer, type::TypeId::VARCHAR, NULL);
		LOG_DEBUG("%s %s", value1.GetInfo().c_str(), value2.GetInfo().c_str());
	}
}

}  // namespace logging
}  // namespace peloton
