#include "gtest/gtest.h"

#include "logging/logging_tests_util.h"

#include "backend/concurrency/transaction_manager.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/data_table.h"
#include "backend/common/value_factory.h"
#include "backend/logging/logmanager.h"
#include "backend/logging/records/tuplerecord.h"

#include "backend/bridge/ddl/ddl_database.h"
#include "backend/storage/database.h"

namespace peloton {
namespace test {

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

  // TODO :: After inserting tuples, store ItemPointers so that we can check in checking part
  InsertTuples(table);

  //TODO:: DeleteTuples(table);

  //TODO:: UpdateTuples(table);

  db->AddTable(table);
  db->DropTableWithOid(table_oid);
  DropDatabase(db_oid);
}

void LoggingTestsUtil::CreateDatabaseAndTable(oid_t db_oid, oid_t table_oid){

  bridge::DDLDatabase::CreateDatabase(db_oid);
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_oid);

  auto table = CreateSimpleTable(db_oid, table_oid);
  db->AddTable(table);

}

void LoggingTestsUtil::DropDatabaseAndTable(oid_t db_oid, oid_t table_oid){
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_oid);
  db->DropTableWithOid(table_oid);
  bridge::DDLDatabase::DropDatabase(db_oid);
}

storage::DataTable* LoggingTestsUtil::CreateSimpleTable(oid_t db_oid, oid_t table_oid){

  auto column_infos = LoggingTestsUtil::CreateSimpleColumns();

  // Construct our schema from vector of ColumnInfo
  auto schema = new catalog::Schema(column_infos);

  storage::DataTable *table = storage::TableFactory::GetDataTable( db_oid, table_oid, schema, std::to_string(table_oid));

  return table;
}

void LoggingTestsUtil::CreateDatabase(oid_t db_oid){
  // Create Database
  bridge::DDLDatabase::CreateDatabase(db_oid);
}

void LoggingTestsUtil::DropDatabase(oid_t db_oid){
  bridge::DDLDatabase::DropDatabase(db_oid);
}

/**
 * @brief Create a simple Column just for convenience
 * @return the vector of Column
 */
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

  sleep(2);

  for( auto tuple : tuples){
    tuple->FreeUninlinedData();
    delete tuple;
  }

  txn_manager.CommitTransaction(txn);
}

void LoggingTestsUtil::CheckTuples(oid_t db_oid, oid_t table_oid){

  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_oid);
  auto table = db->GetTableWithOid(table_oid);

  //TODO:: Check tile group id
  auto TileGroupCount = table->GetTileGroupCount();
  for(oid_t tg_itr=0; tg_itr<TileGroupCount; tg_itr++){
    auto TileGroup = table->GetTileGroup(tg_itr);
    std::cout << "# " << TileGroup->GetTileGroupId() << std::endl;
    auto TileCount = TileGroup->GetTileCount();
    for(oid_t t_itr=0; t_itr<TileCount; t_itr++){
      auto Tile = TileGroup->GetTile(tg_itr);
      std::cout << "## " << Tile->GetTileId() << std::endl;
    }
  }

  auto TileGroup = table->GetTileGroupById(5);
  auto tile = TileGroup->GetTile(6);
  if(0){

  for(oid_t tuple_itr=0; tuple_itr<5; tuple_itr++){
    auto tuple = tile->GetTuple(tuple_itr);
    std::cout << *tuple << std::endl;
  }
  }
}

void LoggingTestsUtil::CheckNextOid(){
    auto &manager = catalog::Manager::GetInstance();
    auto max_oid = manager.GetNextOid();
    //TODO CHange to .. EXPECT_EQ
    EXPECT_EQ(max_oid,8);
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

}  // End test namespace
}  // End peloton namespace
