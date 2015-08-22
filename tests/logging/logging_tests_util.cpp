#include "gtest/gtest.h"
#include "harness.h"

#include "logging/logging_tests_util.h"

#include "backend/bridge/ddl/ddl_database.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/common/value_factory.h"
#include "backend/logging/logmanager.h"
#include "backend/logging/records/tuplerecord.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"

#include <thread>

#define NUM_TUPLES 100
#define NUM_BACKEND 3

namespace peloton {
namespace test {

/**
 * @brief writing a simple log file 
 */
bool LoggingTestsUtil::PrepareLogFile(){

   // start a thread for logging
   auto& logManager = logging::LogManager::GetInstance();

   if( logManager.ActiveFrontendLoggerCount() > 0){
     LOG_ERROR("another logging thread is running now"); 
     return false;
   }
   logManager.SetMainLoggingType(LOGGING_TYPE_ARIES);
   std::thread thread(&logging::LogManager::StandbyLogging, 
                      &logManager, 
                      logManager.GetMainLoggingType());

   // When the frontend logger gets ready to logging,
   // start logging
   while(1){
     sleep(1);
     if( logManager.GetLoggingStatus() == LOGGING_STATUS_TYPE_STANDBY){
       // Standby -> Recovery
       logManager.StartLogging();
       // Recovery -> Ongoing
       break;
     }
   }

   // wait for recovery
   while(logManager.GetLoggingStatus() == LOGGING_STATUS_TYPE_RECOVERY){}

   LoggingTestsUtil::WritingSimpleLog(20000, 10000);

   // ongoing->terminate->sleep
   if( logManager.EndLogging() ){
     thread.join();
     return true;
   }

   LOG_ERROR("Failed to terminate logging thread"); 
   return false;
}

/**
 * @brief recover the database and check the tuples
 */
void LoggingTestsUtil::CheckTupleAfterRecovery(){

  // Initialize oid since we assume that we restart the system
  auto &manager = catalog::Manager::GetInstance();
  manager.SetNextOid(0);
  manager.ClearTileGroup();

  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  txn_manager.ResetStates();

  LoggingTestsUtil::CreateDatabaseAndTable(20000, 10000);

  auto& logManager = logging::LogManager::GetInstance();
   if( logManager.ActiveFrontendLoggerCount() > 0){
     LOG_ERROR("another logging thread is running now"); 
     return false;
   }

  // start a thread for logging
  logManager.SetMainLoggingType(LOGGING_TYPE_ARIES);
  std::thread thread(&logging::LogManager::StandbyLogging, 
      &logManager, 
      logManager.GetMainLoggingType());

  // When the frontend logger gets ready to logging,
  // start logging
  while(1){
    sleep(1);
    if( logManager.GetLoggingStatus() == LOGGING_STATUS_TYPE_STANDBY){
      // Standby -> Recovery
      logManager.StartLogging();
      // Recovery -> Ongoing
      break;
    }
  }

  //wait recovery
  while(1){
    sleep(1);
    // escape when recovery is done
    if( logManager.GetLoggingStatus() == LOGGING_STATUS_TYPE_ONGOING){
      break;
    }
  }

  // Check the tuples
  LoggingTestsUtil::CheckTuples(20000,10000);

  // Check the next oid
  //LoggingTestsUtil::CheckNextOid();

  if( logManager.EndLogging() ){
    thread.join();
  }else{
    LOG_ERROR("Failed to terminate logging thread"); 
  }
  LoggingTestsUtil::DropDatabaseAndTable(20000, 10000);
}


void LoggingTestsUtil::WritingSimpleLog(oid_t db_oid, oid_t table_oid){

  // Create db
  CreateDatabase(db_oid);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_oid);

  // create table, drop it and create again
  // so that table can have two tile groups
  storage::DataTable* table = CreateSimpleTable(db_oid, table_oid);
  db->AddTable(table);
  db->DropTableWithOid(table_oid);
  table = CreateSimpleTable(db_oid, table_oid);

  LaunchParallelTest(NUM_BACKEND, ParallelWriting, table);

  db->AddTable(table);
  db->DropTableWithOid(table_oid);
  DropDatabase(db_oid);
}

void LoggingTestsUtil::CheckTuples(oid_t db_oid, oid_t table_oid){

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
  EXPECT_LT( active_tuple_count, ((NUM_TUPLES-1)*NUM_BACKEND));

}

void LoggingTestsUtil::CreateDatabaseAndTable(oid_t db_oid, oid_t table_oid){

  bridge::DDLDatabase::CreateDatabase(db_oid);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_oid);

  auto table = CreateSimpleTable(db_oid, table_oid);

  db->AddTable(table);

}

void LoggingTestsUtil::CreateDatabase(oid_t db_oid){
  // Create Database
  bridge::DDLDatabase::CreateDatabase(db_oid);
}

storage::DataTable* LoggingTestsUtil::CreateSimpleTable(oid_t db_oid, oid_t table_oid){

  auto column_infos = LoggingTestsUtil::CreateSimpleColumns();

  // Construct our schema from vector of ColumnInfo
  auto schema = new catalog::Schema(column_infos);

  storage::DataTable *table = storage::TableFactory::GetDataTable( db_oid, table_oid, schema, std::to_string(table_oid));

  return table;
}

std::vector<catalog::Column> LoggingTestsUtil::CreateSimpleColumns() {
  // Column
  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, 4, "id");
  catalog::Column column2(VALUE_TYPE_VARCHAR, 68, "name");
  catalog::Column column3(VALUE_TYPE_TIMESTAMP, 8, "time");
  catalog::Column column4(VALUE_TYPE_DOUBLE, 8, "salary");

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);

  return columns;
}

std::vector<storage::Tuple*> LoggingTestsUtil::CreateSimpleTuple(catalog::Schema* schema, oid_t num_of_tuples) {
  
  oid_t tid = (oid_t)GetThreadId();

  std::vector<storage::Tuple*> tuples;

  for (oid_t col_itr = 0; col_itr < num_of_tuples; col_itr++) {
    storage::Tuple *tuple = new storage::Tuple(schema, true);

    // Setting values
    Value integerValue = ValueFactory::GetIntegerValue(243432+col_itr+tid);
    Value stringValue = ValueFactory::GetStringValue("dude"+std::to_string(col_itr+tid));
    Value timestampValue = ValueFactory::GetTimestampValue(10.22+(double)(col_itr+tid));
    Value doubleValue = ValueFactory::GetDoubleValue(244643.1236+(double)(col_itr+tid));

    tuple->SetValue(0, integerValue);
    tuple->SetValue(1, stringValue);
    tuple->SetValue(2, timestampValue);
    tuple->SetValue(3, doubleValue);
    tuples.push_back(tuple);
  }
  return tuples;
}

void LoggingTestsUtil::ParallelWriting(storage::DataTable* table){
  auto locations = InsertTuples(table, true);
  if(locations.size() >= 2)
  DeleteTuples(table, locations[1], true);
  if(locations.size() >= 1)
  UpdateTuples(table, locations[0], true);

  auto& logManager = logging::LogManager::GetInstance();
  if(logManager.IsReadyToLogging()){
    auto logger = logManager.GetBackendLogger();
    // Wait until frontendlogger collect the data
    while( logger->IsWaitFlush()){
      sleep(1);
    }
  }
}

std::vector<ItemPointer> LoggingTestsUtil::InsertTuples(storage::DataTable* table, bool committed){
  std::vector<ItemPointer> locations;

  // Create Tuples
  auto tuples = CreateSimpleTuple(table->GetSchema(),NUM_TUPLES);

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
      auto& logManager = logging::LogManager::GetInstance();
      if(logManager.IsReadyToLogging()){
        auto logger = logManager.GetBackendLogger();

        auto record = new logging::TupleRecord(LOGRECORD_TYPE_TUPLE_INSERT, 
                                               txn->GetTransactionId(), 
                                               table->GetOid(),
                                               location,
                                               tuple,
                                               20000);
        logger->Insert(record);

      }
    }
    if(committed){
      txn_manager.CommitTransaction(txn);
    }
  }

  for( auto tuple : tuples){
    tuple->FreeUninlinedData();
    delete tuple;
  }
  
  return locations;
}

void LoggingTestsUtil::DeleteTuples(storage::DataTable* table, ItemPointer location, bool committed){

  ItemPointer delete_location(location.block,location.offset);

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
    auto& logManager = logging::LogManager::GetInstance();
    if(logManager.IsReadyToLogging()){
      auto logger = logManager.GetBackendLogger();

      auto record = new logging::TupleRecord(LOGRECORD_TYPE_TUPLE_DELETE, 
                                            txn->GetTransactionId(), 
                                            table->GetOid(),
                                            delete_location,
                                            nullptr,
                                            20000);
      logger->Delete(record);
    }
  }

  if(committed){
    txn_manager.CommitTransaction(txn);
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
  auto tuples = CreateSimpleTuple(table->GetSchema(),1);

  for( auto tuple : tuples){
    ItemPointer location = table->UpdateTuple(txn, tuple, delete_location);
    if (location.block == INVALID_OID) {
      txn->SetResult(Result::RESULT_FAILURE);
      continue;
    }
    txn->RecordInsert(location);

    // Logging 
    {
       auto& logManager = logging::LogManager::GetInstance();
       if(logManager.IsReadyToLogging()){
         auto logger = logManager.GetBackendLogger();
   
         auto record = new logging::TupleRecord (LOGRECORD_TYPE_TUPLE_UPDATE, 
                                                 txn->GetTransactionId(), 
                                                 table->GetOid(),
                                                 delete_location,
                                                 tuple,
                                                 20000);
         logger->Update(record);
       }
     }
  }

  if(committed){
    txn_manager.CommitTransaction(txn);
  }

  for( auto tuple : tuples){
    tuple->FreeUninlinedData();
    delete tuple;
  }
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

}  // End test namespace
}  // End peloton namespace
