//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/logger/workload.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <thread>
#include <string>
#include <getopt.h>
#include <sys/stat.h>

#include "backend/bridge/ddl/ddl_database.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/common/value_factory.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/records/transaction_record.h"

#include "backend/benchmark/logger/logger_workload.h"
#include "backend/benchmark/logger/logger_loader.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//

namespace peloton {
namespace benchmark {
namespace logger {

//===--------------------------------------------------------------------===//
// PREPARE LOG FILE
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// 1. Standby -- Bootstrap
// 2. Recovery -- Optional
// 3. Logging -- Collect data and flush when commit
// 4. Terminate -- Collect any remaining data and flush
// 5. Sleep -- Disconnect backend loggers and frontend logger from manager
//===--------------------------------------------------------------------===//

#define LOGGING_TESTS_DATABASE_OID 20000
#define LOGGING_TESTS_TABLE_OID 10000

std::ofstream out("outputfile.summary");

size_t GetLogFileSize();

static void WriteOutput(double value) {
  std::cout << "----------------------------------------------------------\n";
  std::cout << state.logging_type << " " << state.column_count << " "
      << state.tuple_count << " " << state.backend_count << " "
      << state.wait_timeout << " :: ";
  std::cout << value << "\n";

  auto &storage_manager = storage::StorageManager::GetInstance();
  auto& log_manager = logging::LogManager::GetInstance();
  auto frontend_logger = log_manager.GetFrontendLogger();
  auto fsync_count = 0;
  if(frontend_logger != nullptr){
    fsync_count = frontend_logger->GetFsyncCount();
  }

  std::cout << "fsync count : " << fsync_count << " ";
  std::cout << "clflush count : " << storage_manager.GetClflushCount() << " ";
  std::cout << "msync count : " << storage_manager.GetMsyncCount() << "\n";

  out << state.logging_type << " ";
  out << state.column_count << " ";
  out << state.tuple_count << " ";
  out << state.backend_count << " ";
  out << state.wait_timeout << " ";
  out << value << "\n";
  out.flush();
}

std::string GetFilePath(std::string directory_path, std::string file_name) {
  std::string file_path = directory_path;

  // Add a trailing slash to a file path if needed
  if (!file_path.empty() && file_path.back() != '/') file_path += '/';

  file_path += file_name;

  // std::cout << "File Path :: " << file_path << "\n";

  return file_path;
}

/**
 * @brief writing a simple log file
 */
bool PrepareLogFile(std::string file_name) {
  auto file_path = GetFilePath(state.log_file_dir, file_name);

  std::ifstream log_file(file_path);

  // Reset the log file if exists
  if (log_file.good()) {
    std::remove(file_path.c_str());
  }
  log_file.close();

  // start a thread for logging
  auto& log_manager = logging::LogManager::GetInstance();

  if (log_manager.ActiveFrontendLoggerCount() > 0) {
    LOG_ERROR("another logging thread is running now");
    return false;
  }

  // INVALID MODE
  if (peloton_logging_mode == LOGGING_TYPE_INVALID) {
    BuildLog(LOGGING_TESTS_DATABASE_OID,
             LOGGING_TESTS_TABLE_OID);

    return true;
  }

  // TODO:
  std::cout << "Log path :: " << file_path << "\n";

  // set log file and logging type
  log_manager.SetLogFileName(file_path);

  // start off the frontend logger of appropriate type in STANDBY mode
  std::thread thread(&logging::LogManager::StartStandbyMode, &log_manager);

  // wait for the frontend logger to enter STANDBY mode
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_STANDBY, true);

  // STANDBY -> RECOVERY mode
  log_manager.StartRecoveryMode();

  // Wait for the frontend logger to enter LOGGING mode
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_LOGGING, true);

  // Build the log
  BuildLog(LOGGING_TESTS_DATABASE_OID,
           LOGGING_TESTS_TABLE_OID);

  //  Wait for the mode transition :: LOGGING -> TERMINATE -> SLEEP
  if (log_manager.EndLogging()) {
    thread.join();
    return true;
  }

  LOG_ERROR("Failed to terminate logging thread");
  return false;
}

//===--------------------------------------------------------------------===//
// CHECK RECOVERY
//===--------------------------------------------------------------------===//

void ResetSystem() {
  // Initialize oid since we assume that we restart the system
  auto& manager = catalog::Manager::GetInstance();
  manager.SetNextOid(0);
  manager.ClearTileGroup();

  auto& txn_manager = concurrency::TransactionManager::GetInstance();
  txn_manager.ResetStates();
}

/**
 * @brief recover the database and check the tuples
 */
void DoRecovery(std::string file_name) {
  auto file_path = GetFilePath(state.log_file_dir, file_name);

  std::ifstream log_file(file_path);

  // Reset the log file if exists
  log_file.close();

  CreateDatabaseAndTable(LOGGING_TESTS_DATABASE_OID,
                         LOGGING_TESTS_TABLE_OID);

  // start a thread for logging
  auto& log_manager = logging::LogManager::GetInstance();
  if (log_manager.ActiveFrontendLoggerCount() > 0) {
    LOG_ERROR("another logging thread is running now");
    return;
  }

  //===--------------------------------------------------------------------===//
  // RECOVERY
  //===--------------------------------------------------------------------===//

  Timer<std::milli> timer;
  timer.Start();

  // set log file and logging type
  log_manager.SetLogFileName(file_path);

  // start off the frontend logger of appropriate type in STANDBY mode
  std::thread thread(&logging::LogManager::StartStandbyMode, &log_manager);

  // wait for the frontend logger to enter STANDBY mode
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_STANDBY, true);

  // STANDBY -> RECOVERY mode
  log_manager.StartRecoveryMode();

  // Wait for the frontend logger to enter LOGGING mode after recovery
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_LOGGING, true);

  timer.Stop();

  // Recovery time
  if (state.experiment_type == EXPERIMENT_TYPE_RECOVERY)
    WriteOutput(timer.GetDuration());

  // Check the tuple count if needed
  if (state.check_tuple_count) {
    oid_t total_expected = 0;
    CheckTupleCount(LOGGING_TESTS_DATABASE_OID,
                    LOGGING_TESTS_TABLE_OID, total_expected);
  }

  // Check the next oid
  // CheckNextOid();

  if (log_manager.EndLogging()) {
    thread.join();
  } else {
    LOG_ERROR("Failed to terminate logging thread");
  }
  DropDatabaseAndTable(LOGGING_TESTS_DATABASE_OID,
                       LOGGING_TESTS_TABLE_OID);
}

void CheckTupleCount(oid_t db_oid, oid_t table_oid,
                     oid_t expected) {
  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(db_oid);
  auto table = db->GetTableWithOid(table_oid);

  oid_t tile_group_count = table->GetTileGroupCount();

  oid_t active_tuple_count = 0;
  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
      tile_group_itr++) {
    auto tile_group = table->GetTileGroup(tile_group_itr);
    active_tuple_count += tile_group->GetActiveTupleCount(INVALID_TXN_ID);
  }

  // check # of active tuples
  if(expected != active_tuple_count) {
    throw Exception("Tuple count does not match \n");
  }
}

//===--------------------------------------------------------------------===//
// WRITING LOG RECORD
//===--------------------------------------------------------------------===//

void BuildLog(oid_t db_oid, oid_t table_oid) {
  // Build a pool
  auto logging_pool = new VarlenPool(BACKEND_TYPE_MM);

  // Create db
  CreateDatabase(db_oid);
  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(db_oid);

  // Create table, drop it and create again
  // so that table can have a newly added tile group and
  // not just the default tile group
  storage::DataTable* table = CreateUserTable(db_oid, table_oid);
  db->AddTable(table);

  // Tuple count
  oid_t per_backend_tuple_count = state.tuple_count / state.backend_count;

  // Create Tuples
  auto tuples =
      CreateTuples(table->GetSchema(), per_backend_tuple_count, logging_pool);

  //===--------------------------------------------------------------------===//
  // ACTIVE PROCESSING
  //===--------------------------------------------------------------------===//
  Timer<std::milli> timer;
  timer.Start();

  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(RunBackends, table, tuples));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  timer.Stop();

  // Build log time
  if (state.experiment_type == EXPERIMENT_TYPE_ACTIVE ||
      state.experiment_type == EXPERIMENT_TYPE_WAIT) {
    WriteOutput(timer.GetDuration());
  } else if (state.experiment_type == EXPERIMENT_TYPE_STORAGE) {
    auto log_file_size = GetLogFileSize();
    std::cout << "Log file size :: " << log_file_size << "\n";
    WriteOutput(log_file_size);
  }

  // Clean up data
  for (auto tuple : tuples) {
    delete tuple;
  }

  // Check the tuple count if needed
  if (state.check_tuple_count) {
    oid_t total_expected = 0;
    CheckTupleCount(db_oid, table_oid, total_expected);
  }

  // We can only drop the table in case of WAL
  if (IsBasedOnWriteAheadLogging(peloton_logging_mode) == true) {
    db->DropTableWithOid(table_oid);
    DropDatabase(db_oid);
  }
}

void RunBackends(storage::DataTable* table,
                 const std::vector<storage::Tuple*>& tuples) {
  bool commit = true;

  // Insert tuples
  auto locations = InsertTuples(table, tuples, commit);

  // Update tuples
  locations = UpdateTuples(table, locations, tuples, commit);

  // Delete tuples
  DeleteTuples(table, locations, commit);

  // Remove the backend logger after flushing out all the changes
  auto& log_manager = logging::LogManager::GetInstance();
  if (log_manager.IsInLoggingMode()) {
    auto logger = log_manager.GetBackendLogger();

    // Wait until frontend logger collects the data
    logger->WaitForFlushing();

    log_manager.RemoveBackendLogger(logger);
  }
}

// Do insert and create insert tuple log records
std::vector<ItemPointer> InsertTuples(
    storage::DataTable* table, const std::vector<storage::Tuple*>& tuples,
    bool committed) {
  std::vector<ItemPointer> locations;

  auto& txn_manager = concurrency::TransactionManager::GetInstance();

  for (auto tuple : tuples) {
    auto txn = txn_manager.BeginTransaction();
    ItemPointer location = table->InsertTuple(txn, tuple);
    if (location.block == INVALID_OID) {
      txn->SetResult(Result::RESULT_FAILURE);
      std::cout << "Insert failed \n";
      exit(EXIT_FAILURE);
    }

    txn->RecordInsert(location);

    locations.push_back(location);

    // Logging
    {
      auto& log_manager = logging::LogManager::GetInstance();

      if (log_manager.IsInLoggingMode()) {
        auto logger = log_manager.GetBackendLogger();
        auto record = logger->GetTupleRecord(
            LOGRECORD_TYPE_TUPLE_INSERT, txn->GetTransactionId(),
            table->GetOid(), location, INVALID_ITEMPOINTER, tuple,
            LOGGING_TESTS_DATABASE_OID);
        logger->Log(record);
      }
    }

    // commit or abort as required
    if (committed) {
      txn_manager.CommitTransaction();
    } else {
      txn_manager.AbortTransaction();
    }
  }

  return locations;
}

void DeleteTuples(storage::DataTable* table,
                  const std::vector<ItemPointer>& locations,
                  bool committed) {
  for (auto delete_location : locations) {
    auto& txn_manager = concurrency::TransactionManager::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    bool status = table->DeleteTuple(txn, delete_location);
    if (status == false) {
      txn->SetResult(Result::RESULT_FAILURE);
      std::cout << "Delete failed \n";
      exit(EXIT_FAILURE);
    }

    txn->RecordDelete(delete_location);

    // Logging
    {
      auto& log_manager = logging::LogManager::GetInstance();

      if (log_manager.IsInLoggingMode()) {
        auto logger = log_manager.GetBackendLogger();
        auto record = logger->GetTupleRecord(
            LOGRECORD_TYPE_TUPLE_DELETE, txn->GetTransactionId(),
            table->GetOid(), INVALID_ITEMPOINTER, delete_location, nullptr,
            LOGGING_TESTS_DATABASE_OID);
        logger->Log(record);
      }
    }

    if (committed) {
      txn_manager.CommitTransaction();
    } else {
      txn_manager.AbortTransaction();
    }
  }
}

std::vector<ItemPointer> UpdateTuples(
    storage::DataTable* table,
    const std::vector<ItemPointer>& deleted_locations,
    const std::vector<storage::Tuple*>& tuples, bool committed) {
  // Inserted locations
  std::vector<ItemPointer> inserted_locations;

  size_t tuple_itr = 0;
  for (auto delete_location : deleted_locations) {
    auto tuple = tuples[tuple_itr];
    tuple_itr++;

    auto& txn_manager = concurrency::TransactionManager::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    bool status = table->DeleteTuple(txn, delete_location);
    if (status == false) {
      txn->SetResult(Result::RESULT_FAILURE);
      std::cout << "Delete failed \n";
      exit(EXIT_FAILURE);
    }

    txn->RecordDelete(delete_location);

    ItemPointer insert_location = table->InsertTuple(txn, tuple);
    if (insert_location.block == INVALID_OID) {
      txn->SetResult(Result::RESULT_FAILURE);
      std::cout << "Insert failed \n";
      exit(EXIT_FAILURE);
    }
    txn->RecordInsert(insert_location);

    inserted_locations.push_back(insert_location);

    // Logging
    {
      auto& log_manager = logging::LogManager::GetInstance();
      if (log_manager.IsInLoggingMode()) {
        auto logger = log_manager.GetBackendLogger();
        auto record = logger->GetTupleRecord(
            LOGRECORD_TYPE_TUPLE_UPDATE, txn->GetTransactionId(),
            table->GetOid(), insert_location, delete_location, tuple,
            LOGGING_TESTS_DATABASE_OID);
        logger->Log(record);
      }
    }

    if (committed) {
      txn_manager.CommitTransaction();
    } else {
      txn_manager.AbortTransaction();
    }
  }

  return inserted_locations;
}

//===--------------------------------------------------------------------===//
// Utility functions
//===--------------------------------------------------------------------===//

void CreateDatabaseAndTable(oid_t db_oid, oid_t table_oid) {
  // Create database and attach a table
  bridge::DDLDatabase::CreateDatabase(db_oid);
  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(db_oid);

  auto table = CreateUserTable(db_oid, table_oid);

  db->AddTable(table);
}

storage::DataTable* CreateUserTable(oid_t db_oid,
                                    oid_t table_oid) {
  auto column_infos = CreateSchema();

  bool own_schema = true;
  bool adapt_table = false;
  const int tuples_per_tilegroup_count = DEFAULT_TUPLES_PER_TILEGROUP;

  // Construct our schema from vector of ColumnInfo
  auto schema = new catalog::Schema(column_infos);
  storage::DataTable* table = storage::TableFactory::GetDataTable(
      db_oid, table_oid, schema, "USERTABLE", tuples_per_tilegroup_count,
      own_schema, adapt_table);

  return table;
}

void CreateDatabase(oid_t db_oid) {
  // Create Database
  bridge::DDLDatabase::CreateDatabase(db_oid);
}

std::vector<catalog::Column> CreateSchema() {
  // Columns
  std::vector<catalog::Column> columns;
  const size_t field_length = 100;
  const bool is_inlined = true;

  // User Id
  catalog::Column user_id(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "YCSB_KEY", is_inlined);

  columns.push_back(user_id);

  // Field
  for (oid_t col_itr = 0; col_itr < state.column_count; col_itr++) {
    catalog::Column field(VALUE_TYPE_VARCHAR, field_length,
                          "FIELD" + std::to_string(col_itr), is_inlined);

    columns.push_back(field);
  }

  return columns;
}

std::vector<storage::Tuple*> CreateTuples(
    catalog::Schema* schema, oid_t num_of_tuples, VarlenPool* pool) {
  std::vector<storage::Tuple*> tuples;
  const bool allocate = true;
  const size_t string_length = 100;
  std::string dummy_string('-', string_length);

  for (oid_t tuple_itr = 0; tuple_itr < num_of_tuples; tuple_itr++) {
    // Build tuple
    storage::Tuple* tuple = new storage::Tuple(schema, allocate);

    Value user_id_value = ValueFactory::GetIntegerValue(tuple_itr);
    tuple->SetValue(0, user_id_value, nullptr);

    for (oid_t col_itr = 1; col_itr < state.column_count; col_itr++) {
      Value field_value = ValueFactory::GetStringValue(dummy_string, pool);
      tuple->SetValue(col_itr, field_value, pool);
    }

    tuples.push_back(tuple);
  }

  return tuples;
}

void DropDatabaseAndTable(oid_t db_oid, oid_t table_oid) {
  auto& manager = catalog::Manager::GetInstance();

  storage::Database* db = manager.GetDatabaseWithOid(db_oid);
  db->DropTableWithOid(table_oid);

  bridge::DDLDatabase::DropDatabase(db_oid);
}

void DropDatabase(oid_t db_oid) {
  bridge::DDLDatabase::DropDatabase(db_oid);
}

size_t GetLogFileSize() {
  struct stat log_stats;

  auto& log_manager = logging::LogManager::GetInstance();
  std::string log_file_name = log_manager.GetLogFileName();

  // open log file and file descriptor
  // we open it in append + binary mode
  auto log_file = fopen(log_file_name.c_str(), "r");
  if (log_file == NULL) {
    LOG_ERROR("LogFile is NULL");
  }

  // also, get the descriptor
  auto log_file_fd = fileno(log_file);
  if (log_file_fd == -1) {
    LOG_ERROR("log_file_fd is -1");
  }

  fstat(log_file_fd, &log_stats);
  auto log_file_size = log_stats.st_size;

  return log_file_size;
}

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
