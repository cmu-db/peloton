#include "gtest/gtest.h"

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

namespace peloton {
namespace test {

/**
 * @brief writing a simple log file
 */
void LoggingTestsUtil::PrepareLogFile(){

   // start a thread for logging
   auto& logManager = logging::LogManager::GetInstance();
   logManager.SetMainLoggingType(LOGGING_TYPE_ARIES);
   std::thread thread(&logging::LogManager::StandbyLogging, 
                      &logManager, 
                      logManager.GetMainLoggingType());

   // When the frontend logger gets ready to logging,
   // start logging
   while(1){
     if( logManager.GetLoggingStatus() == LOGGING_STATUS_TYPE_STANDBY){
       logManager.StartLogging();
       break;
     }
   }

   LoggingTestsUtil::WritingSimpleLog(20000, 10000);

   sleep(1);

   if( logManager.EndLogging() ){
     thread.join();
   }else{
     LOG_ERROR("Failed to terminate logging thread"); 
   }
}

/**
 * @brief recover the database and check the tuples
 */
void LoggingTestsUtil::CheckTupleAfterRecovery(){

  LoggingTestsUtil::CreateDatabaseAndTable(20000, 10000);

  // start a thread for logging
  auto& logManager = logging::LogManager::GetInstance();
  logManager.SetMainLoggingType(LOGGING_TYPE_ARIES);
  std::thread thread(&logging::LogManager::StandbyLogging, 
      &logManager, 
      logManager.GetMainLoggingType());

  // When the frontend logger gets ready to logging,
  // start logging
  while(1){
    if( logManager.GetLoggingStatus() == LOGGING_STATUS_TYPE_STANDBY){
      // Create corresponding database and table
      logManager.StartLogging(); // Change status to recovery
      break;
    }
  }

  //wait recovery
  while(1){
    // escape when recovery is done
    if( logManager.GetLoggingStatus() == LOGGING_STATUS_TYPE_ONGOING){
      break;
    }
  }


  sleep(2);

  // Check the tuples
  LoggingTestsUtil::CheckTuples(20000,10000);

  sleep(2);

  // Check the next oid
  LoggingTestsUtil::CheckNextOid();

  sleep(2);

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

  InsertTuples(table);

  //TODO:: DeleteTuples(table);

  //TODO:: UpdateTuples(table);

  db->AddTable(table);
  db->DropTableWithOid(table_oid);
  DropDatabase(db_oid);
}

void LoggingTestsUtil::CheckTuples(oid_t db_oid, oid_t table_oid){

  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_oid);
  auto table = db->GetTableWithOid(table_oid);

  //FIXME :: Tile Group has invalid db oid
  auto tile_group = table->GetTileGroupById(5);

  auto tile = tile_group->GetTile((oid_t)0);

  Value integerValue = ValueFactory::GetIntegerValue(243432);
  Value stringValue = ValueFactory::GetStringValue("dude0");
  Value timestampValue = ValueFactory::GetTimestampValue(10.22);
  Value doubleValue = ValueFactory::GetDoubleValue(244643.1236);

  EXPECT_EQ(tile->GetValue(0,0), integerValue);
  EXPECT_EQ(tile->GetValue(0,1), stringValue);
  EXPECT_EQ(tile->GetValue(0,2), timestampValue);
  EXPECT_EQ(tile->GetValue(0,3), doubleValue);

  // Make valgrind happy
  stringValue.FreeUninlinedData();
}

void LoggingTestsUtil::CheckNextOid(){
    auto &manager = catalog::Manager::GetInstance();
    auto max_oid = manager.GetNextOid();
    EXPECT_EQ(max_oid,8);
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

std::vector<storage::Tuple*> LoggingTestsUtil::CreateSimpleTuples(catalog::Schema* schema) {
  std::vector<storage::Tuple*> tuples;

  for (int col_itr = 0; col_itr < 5; col_itr++) {
    storage::Tuple *tuple = new storage::Tuple(schema, true);

    // Setting values
    Value integerValue = ValueFactory::GetIntegerValue(243432+col_itr);
    Value stringValue = ValueFactory::GetStringValue("dude"+std::to_string(col_itr));
    Value timestampValue = ValueFactory::GetTimestampValue(10.22);
    Value doubleValue = ValueFactory::GetDoubleValue(244643.1236+(double)(col_itr));

    tuple->SetValue(0, integerValue);
    tuple->SetValue(1, stringValue);
    tuple->SetValue(2, timestampValue);
    tuple->SetValue(3, doubleValue);
    tuples.push_back(tuple);
  }
  return tuples;
}

void LoggingTestsUtil::InsertTuples(storage::DataTable* table){

  // Create Tuples
  auto tuples = CreateSimpleTuples(table->GetSchema());

  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  for( auto tuple : tuples){
    ItemPointer location = table->InsertTuple(txn, tuple);
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
  }

  std::cout << *table << std::endl;

  sleep(2);

  for( auto tuple : tuples){
    tuple->FreeUninlinedData();
    delete tuple;
  }

  txn_manager.CommitTransaction(txn);
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
