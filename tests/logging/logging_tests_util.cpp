#include "gtest/gtest.h"
#include "harness.h"

#include <thread>

#include "logging/logging_tests_util.h"

#include "backend/bridge/ddl/ddl_database.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/common/value_factory.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
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
  if (DoTestSuspendCommit()) {
    log_manager.SetTestInterruptCommit(true);
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
  LoggingTestsUtil::CreateDatabaseAndTable(20000, 10000);

  auto& log_manager = logging::LogManager::GetInstance();
  if( log_manager.ActiveFrontendLoggerCount() > 0){
    LOG_ERROR("another logging thread is running now");
    return;
  }
  log_manager.SetLogFile(log_file);
  // start a thread for logging
  log_manager.SetDefaultLoggingType(logging_type);

  std::thread thread(&logging::LogManager::StartStandbyMode, 
                     &log_manager,
                     log_manager.GetDefaultLoggingType());

  // When the frontend logger gets ready to logging,
  // start logging
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_STANDBY);

  // always enable commit when recovery
  log_manager.SetTestInterruptCommit(false);

  // Standby -> Recovery
  log_manager.StartRecoveryMode();

  // Recovery -> Ongoing

  //wait recovery
  log_manager.WaitForMode(LOGGING_STATUS_TYPE_LOGGING);

  if (DoCheckTupleNumber()) {
    // Check the tuples
    LoggingTestsUtil::CheckTupleCount(20000, 10000,
                                      ((GetTestTupleNumber()-1) * GetTestThreadNumber()));
  }

  // Check the next oid
  //LoggingTestsUtil::CheckNextOid();

  if( log_manager.EndLogging() ){
    thread.join();
  }else{
    LOG_ERROR("Failed to terminate logging thread");
  }
  LoggingTestsUtil::DropDatabaseAndTable(20000, 10000);
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
  db->DropTableWithOid(table_oid);
  table = CreateSimpleTable(db_oid, table_oid);

  // Execute the workload to build the log
  LaunchParallelTest(GetTestThreadNumber(), RunBackends, table);

  db->AddTable(table);

  // Check the tuple count if needed
  auto check_tuple_number = DoCheckTupleNumber();
  if (check_tuple_number) {
    oid_t per_thread_expected = GetTestTupleNumber() - 1;
    oid_t total_expected =  per_thread_expected * GetTestThreadNumber();

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
  auto tuples = CreateTuples(table->GetSchema(), GetTestTupleNumber());

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

oid_t LoggingTestsUtil::GetTestTupleNumber() {
  char* tuples_number_str = getenv("NUM_TUPLES");
  if (tuples_number_str) {
    return atof(tuples_number_str);
  } else {
    return 20;
  }
}

bool LoggingTestsUtil::DoCheckTupleNumber() {
  char* check_tuple_str = getenv("CHECK_TUPLES_NUM");
  if (check_tuple_str) {
    if(atoi(check_tuple_str) == 0) {
      return false;
    }
  }
  return true;
}

// SUSPEND_COMMIT: suspend final step in transaction commit,
// so that it only get committed during recovery
bool LoggingTestsUtil::DoTestSuspendCommit() {
  char* suspend_commit_str = getenv("SUSPEND_COMMIT");
  if (suspend_commit_str) {
    if(atoi(suspend_commit_str) != 0) {
      return true;
    }
  }
  return false;
}

uint LoggingTestsUtil::GetTestThreadNumber() {
  char* thread_number_str = getenv("NUM_BACKEND");
  if (thread_number_str) {
    return atoi(thread_number_str);
  } else {
    return 4;
  }
}

}  // End test namespace
}  // End peloton namespace
