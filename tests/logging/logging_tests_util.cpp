#include "gtest/gtest.h"
#include "harness.h"

#include <thread>
#include <getopt.h>

#include "logging/logging_tests_util.h"

#include "backend/bridge/ddl/ddl_database.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/common/value_factory.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tuple.h"
#include "backend/storage/tile_group.h"
#include "backend/logging/log_manager.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/records/transaction_record.h"

namespace peloton {
namespace test {

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
#define LOGGING_TESTS_TABLE_OID    10000

// configuration for testing
LoggingTestsUtil::logging_test_configuration state;

/**
 * @brief writing a simple log file 
 */
bool LoggingTestsUtil::PrepareLogFile(LoggingType logging_type,
                                      std::string log_file){

  // start a thread for logging
  auto& log_manager = logging::LogManager::GetInstance();

  if( log_manager.ActiveFrontendLoggerCount() > 0){
    LOG_ERROR("another logging thread is running now");
    return false;
  }

  // set log file and logging type
  log_manager.SetLogFile(log_file);
  log_manager.SetDefaultLoggingType(logging_type);

  // start off the frontend logger of appropriate type in STANDBY mode
  std::thread thread(&logging::LogManager::StartStandbyMode,
                     &log_manager,
                     log_manager.GetDefaultLoggingType());

  // wait for the frontend logger to enter STANDBY mode
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_STANDBY);

  // suspend final step in transaction commit,
  // so that it only get committed during recovery
  if (state.redo_all) {
    log_manager.SetTestRedoAllLogs(true);
  }

  // STANDBY -> RECOVERY mode
  log_manager.StartRecoveryMode();

  // Wait for the frontend logger to enter LOGGING mode
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_LOGGING);

  // Build the log
  LoggingTestsUtil::BuildLog(LOGGING_TESTS_DATABASE_OID,
                             LOGGING_TESTS_TABLE_OID,
                             logging_type);

  //  Wait for the mode transition :: LOGGING -> TERMINATE -> SLEEP
  if(log_manager.EndLogging()){
    thread.join();
    return true;
  }

  LOG_ERROR("Failed to terminate logging thread");
  return false;
}

//===--------------------------------------------------------------------===//
// CHECK RECOVERY
//===--------------------------------------------------------------------===//

void LoggingTestsUtil::ResetSystem(){
  // Initialize oid since we assume that we restart the system
  auto &manager = catalog::Manager::GetInstance();
  manager.SetNextOid(0);
  manager.ClearTileGroup();

  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  txn_manager.ResetStates();
}

/**
 * @brief recover the database and check the tuples
 */
void LoggingTestsUtil::CheckRecovery(LoggingType logging_type,
                                     std::string log_file){
  LoggingTestsUtil::CreateDatabaseAndTable(LOGGING_TESTS_DATABASE_OID, LOGGING_TESTS_TABLE_OID);

  // start a thread for logging
  auto& log_manager = logging::LogManager::GetInstance();
  if( log_manager.ActiveFrontendLoggerCount() > 0){
    LOG_ERROR("another logging thread is running now");
    return;
  }

  // set log file and logging type
  log_manager.SetLogFile(log_file);
  log_manager.SetDefaultLoggingType(logging_type);

  // start off the frontend logger of appropriate type in STANDBY mode
  std::thread thread(&logging::LogManager::StartStandbyMode, 
                     &log_manager,
                     log_manager.GetDefaultLoggingType());

  // wait for the frontend logger to enter STANDBY mode
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_STANDBY);

  // always enable commit when testing recovery
  if (state.redo_all) {
    log_manager.SetTestRedoAllLogs(true);
  }

  // STANDBY -> RECOVERY mode
  log_manager.StartRecoveryMode();

  // Wait for the frontend logger to enter LOGGING mode after recovery
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_LOGGING);

  // Check the tuple count if needed
  if (state.check_tuple_count) {
    oid_t per_thread_expected = state.tuple_count - 1;
    oid_t total_expected =  per_thread_expected * state.backend_count;

    LoggingTestsUtil::CheckTupleCount(LOGGING_TESTS_DATABASE_OID,
                                      LOGGING_TESTS_TABLE_OID,
                                      total_expected);
  }

  // Check the next oid
  //LoggingTestsUtil::CheckNextOid();

  if( log_manager.EndLogging() ){
    thread.join();
  }else{
    LOG_ERROR("Failed to terminate logging thread");
  }
  LoggingTestsUtil::DropDatabaseAndTable(LOGGING_TESTS_DATABASE_OID, LOGGING_TESTS_TABLE_OID);
}

void LoggingTestsUtil::CheckTupleCount(oid_t db_oid, oid_t table_oid, oid_t expected){

  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_oid);
  auto table = db->GetTableWithOid(table_oid);

  oid_t tile_group_count = table->GetTileGroupCount();
  oid_t active_tuple_count = 0;
  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
      tile_group_itr++) {
    auto tile_group = table->GetTileGroup(tile_group_itr);
    active_tuple_count += tile_group->GetActiveTupleCount();
  }

  // check # of active tuples
  EXPECT_EQ(expected, active_tuple_count);
}

//===--------------------------------------------------------------------===//
// WRITING LOG RECORD
//===--------------------------------------------------------------------===//

void LoggingTestsUtil::BuildLog(oid_t db_oid, oid_t table_oid,
                                LoggingType logging_type){

  // Create db
  CreateDatabase(db_oid);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_oid);

  // Create table, drop it and create again
  // so that table can have a newly added tile group and
  // not just the default tile group
  storage::DataTable* table = CreateSimpleTable(db_oid, table_oid);
  db->AddTable(table);

  // Execute the workload to build the log
  LaunchParallelTest(state.backend_count, RunBackends, table);

  // Check the tuple count if needed
  if (state.check_tuple_count) {
    oid_t per_thread_expected = state.tuple_count - 1;
    oid_t total_expected =  per_thread_expected * state.backend_count;

    LoggingTestsUtil::CheckTupleCount(db_oid, table_oid, total_expected);
  }

  // We can only drop the table in case of ARIES
  if(logging_type == LOGGING_TYPE_ARIES){
    db->DropTableWithOid(table_oid);
    DropDatabase(db_oid);
  }
}


void LoggingTestsUtil::RunBackends(storage::DataTable* table){

  bool commit = true;
  auto locations = InsertTuples(table, commit);

  // Delete the second inserted location if we insert >= 2 tuples
  if(locations.size() >= 2)
    DeleteTuples(table, locations[1], commit);

  // Update the first inserted location if we insert >= 1 tuples
  if(locations.size() >= 1)
    UpdateTuples(table, locations[0], commit);

  // This insert should have no effect
  commit = false;
  InsertTuples(table, commit);

  // Remove the backend logger after flushing out all the changes
  auto& log_manager = logging::LogManager::GetInstance();
  if(log_manager.IsInLoggingMode()){
    auto logger = log_manager.GetBackendLogger();

    // Wait until frontend logger collects the data
    logger->WaitForFlushing();

    log_manager.RemoveBackendLogger(logger);
  }

}

// Do insert and create insert tuple log records
std::vector<ItemPointer> LoggingTestsUtil::InsertTuples(storage::DataTable* table, bool committed){
  std::vector<ItemPointer> locations;

  // Create Tuples
  auto tuples = CreateTuples(table->GetSchema(), state.tuple_count);

  auto &txn_manager = concurrency::TransactionManager::GetInstance();

  for( auto tuple : tuples){
    auto txn = txn_manager.BeginTransaction();
    ItemPointer location = table->InsertTuple(txn, tuple);
    if (location.block == INVALID_OID) {
      txn->SetResult(Result::RESULT_FAILURE);
      continue;
    }

    locations.push_back(location);

    txn->RecordInsert(location);

    // Logging 
    {
      auto& log_manager = logging::LogManager::GetInstance();

      if(log_manager.IsInLoggingMode()){
        auto logger = log_manager.GetBackendLogger();
        auto record = logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_INSERT,
                                             txn->GetTransactionId(), 
                                             table->GetOid(),
                                             location,
                                             INVALID_ITEMPOINTER,
                                             tuple,
                                             LOGGING_TESTS_DATABASE_OID);
        logger->Log(record);

      }
    }

    // commit or abort as required
    if(committed){
      txn_manager.CommitTransaction();
    } else{
      txn_manager.AbortTransaction();
    }
  }

  // Clean up data
  for( auto tuple : tuples){
    tuple->FreeUninlinedData();
    delete tuple;
  }

  return locations;
}

void LoggingTestsUtil::DeleteTuples(storage::DataTable* table, 
                                    ItemPointer location, 
                                    bool committed){

  // Location of tuple that needs to be deleted
  ItemPointer delete_location(location.block, location.offset);

  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  bool status = table->DeleteTuple(txn, delete_location);
  if (status == false) {
    txn->SetResult(Result::RESULT_FAILURE);
    return;
  }

  txn->RecordDelete(delete_location);

  // Logging 
  {
    auto& log_manager = logging::LogManager::GetInstance();

    if(log_manager.IsInLoggingMode()){
      auto logger = log_manager.GetBackendLogger();
      auto record = logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_DELETE,
                                           txn->GetTransactionId(),
                                           table->GetOid(),
                                           INVALID_ITEMPOINTER,
                                           delete_location,
                                           nullptr,
                                           LOGGING_TESTS_DATABASE_OID);
      logger->Log(record);
    }
  }

  if(committed){
    txn_manager.CommitTransaction();
  }else{
    txn_manager.AbortTransaction();
  }
}

void LoggingTestsUtil::UpdateTuples(storage::DataTable* table, ItemPointer location, bool committed){

  ItemPointer delete_location(location.block,location.offset);

  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  bool status = table->DeleteTuple(txn, delete_location);
  if (status == false) {
    txn->SetResult(Result::RESULT_FAILURE);
    return;
  }

  txn->RecordDelete(delete_location);

  // Create Tuples
  oid_t tuple_count = 1;
  auto tuples = CreateTuples(table->GetSchema(), tuple_count);

  for( auto tuple : tuples){
    ItemPointer location = table->InsertTuple(txn, tuple);
    if (location.block == INVALID_OID) {
      txn->SetResult(Result::RESULT_FAILURE);
      continue;
    }
    txn->RecordInsert(location);

    // Logging 
    {
      auto& log_manager = logging::LogManager::GetInstance();
      if(log_manager.IsInLoggingMode()){
        auto logger = log_manager.GetBackendLogger();
        auto record = logger->GetTupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE,
                                             txn->GetTransactionId(), 
                                             table->GetOid(),
                                             location,
                                             delete_location,
                                             tuple,
                                             LOGGING_TESTS_DATABASE_OID);
        logger->Log(record);
      }
    }
  }

  if(committed){
    txn_manager.CommitTransaction();
  } else{
    txn_manager.AbortTransaction();
  }

  // Clean up data
  for( auto tuple : tuples){
    tuple->FreeUninlinedData();
    delete tuple;
  }
}

//===--------------------------------------------------------------------===//
// Utility functions
//===--------------------------------------------------------------------===//

void LoggingTestsUtil::CreateDatabaseAndTable(oid_t db_oid, oid_t table_oid){

  // Create database and attach a table
  bridge::DDLDatabase::CreateDatabase(db_oid);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_oid);

  auto table = CreateSimpleTable(db_oid, table_oid);

  db->AddTable(table);
}

storage::DataTable* LoggingTestsUtil::CreateSimpleTable(oid_t db_oid, oid_t table_oid){

  auto column_infos = LoggingTestsUtil::CreateSchema();

  bool own_schema = true;
  bool adapt_table = false;
  const int tuples_per_tilegroup_count = 10;

  // Construct our schema from vector of ColumnInfo
  auto schema = new catalog::Schema(column_infos);
  storage::DataTable *table = storage::TableFactory::GetDataTable(db_oid, table_oid,
                                                                  schema,
                                                                  std::to_string(table_oid),
                                                                  tuples_per_tilegroup_count,
                                                                  own_schema,
                                                                  adapt_table);

  return table;
}

void LoggingTestsUtil::CreateDatabase(oid_t db_oid){
  // Create Database
  bridge::DDLDatabase::CreateDatabase(db_oid);
}

std::vector<catalog::Column> LoggingTestsUtil::CreateSchema() {
  // Column
  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_BIGINT, 8, "id");
  catalog::Column column2(VALUE_TYPE_VARCHAR, 68, "name");
  catalog::Column column3(VALUE_TYPE_TIMESTAMP, 8, "time");
  catalog::Column column4(VALUE_TYPE_DOUBLE, 8, "salary");

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);

  return columns;
}

std::vector<storage::Tuple*> LoggingTestsUtil::CreateTuples(catalog::Schema* schema, oid_t num_of_tuples) {

  oid_t thread_id = (oid_t) GetThreadId();

  std::vector<storage::Tuple*> tuples;

  for (oid_t col_itr = 0; col_itr < num_of_tuples; col_itr++) {
    storage::Tuple *tuple = new storage::Tuple(schema, true);

    // Setting values in tuple
    Value longValue = ValueFactory::GetBigIntValue(243432l+col_itr+thread_id);
    Value stringValue = ValueFactory::GetStringValue("dude"+std::to_string(col_itr+thread_id));
    Value timestampValue = ValueFactory::GetTimestampValue(10.22+(double)(col_itr+thread_id));
    Value doubleValue = ValueFactory::GetDoubleValue(244643.1236+(double)(col_itr+thread_id));

    tuple->SetValue(0, longValue);
    tuple->SetValue(1, stringValue);
    tuple->SetValue(2, timestampValue);
    tuple->SetValue(3, doubleValue);
    tuples.push_back(tuple);
  }

  return tuples;
}

void LoggingTestsUtil::DropDatabaseAndTable(oid_t db_oid, oid_t table_oid){
  auto &manager = catalog::Manager::GetInstance();

  storage::Database *db = manager.GetDatabaseWithOid(db_oid);
  db->DropTableWithOid(table_oid);

  bridge::DDLDatabase::DropDatabase(db_oid);
}

void LoggingTestsUtil::DropDatabase(oid_t db_oid){
  bridge::DDLDatabase::DropDatabase(db_oid);
}

//===--------------------------------------------------------------------===//
// Configuration
//===--------------------------------------------------------------------===//

static void Usage(FILE *out) {
  fprintf(out, "Command line options : hyadapt <options> \n"
          "   -h --help              :  Print help message \n"
          "   -t --tuple-count       :  Tuple count \n"
          "   -b --backend-count     :  Backend count \n"
          "   -z --tuple-size        :  Tuple size (does not work) \n"
          "   -c --check-tuple-count :  Check tuple count \n"
          "   -r --redo-all-logs     :  Redo all logs \n"
  );
  exit(EXIT_FAILURE);
}

static struct option opts[] = {
    { "tuple-count", optional_argument, NULL, 't' },
    { "backend-count", optional_argument, NULL, 'b' },
    { "tuple-size", optional_argument, NULL, 'z' },
    { "check-tuple-count", optional_argument, NULL, 'c' },
    { "redo-all-logs", optional_argument, NULL, 'r' },
    { NULL, 0, NULL, 0 }
};

static void PrintConfiguration(){
  int width = 25;

  std::cout << std::setw(width) << std::left
      << "tuple_count " << " : " << state.tuple_count << std::endl;
  std::cout << std::setw(width) << std::left
      << "backend_count " << " : " << state.backend_count << std::endl;
  std::cout << std::setw(width) << std::left
      << "tuple_size " << " : " << state.tuple_size << std::endl;
  std::cout << std::setw(width) << std::left
      << "check_tuple_count " << " : " << state.check_tuple_count << std::endl;
  std::cout << std::setw(width) << std::left
      << "redo_all_logs " << " : " << state.redo_all << std::endl;

}

void LoggingTestsUtil::ParseArguments(int argc, char* argv[]) {

  // Default Values
  state.tuple_count = 20;

  state.backend_count = 4;

  state.tuple_size = 100;

  state.check_tuple_count = true;
  state.redo_all = false;

  // Parse args
  while (1) {
    int idx = 0;
    int c = getopt_long(argc, argv, "aht:b:z:c:r:", opts,
                        &idx);

    if (c == -1)
      break;

    switch (c) {
      case 't':
        state.tuple_count  = atoi(optarg);
        break;
      case 'b':
        state.backend_count  = atoi(optarg);
        break;
      case 'z':
        state.tuple_size  = atoi(optarg);
        break;
      case 'c':
        state.check_tuple_count  = atoi(optarg);
        break;
      case 'r':
        state.redo_all  = atoi(optarg);
        break;

      case 'h':
        Usage(stderr);
        break;

      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        Usage(stderr);
    }
  }

  PrintConfiguration();

}


}  // End test namespace
}  // End peloton namespace
