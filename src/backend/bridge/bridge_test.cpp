/*-------------------------------------------------------------------------
 *
 * plan_transformer_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * Author: Ming Fang
 *
 *-------------------------------------------------------------------------
 */

#include "bridge_test.h"


#include "backend/bridge/bridge.h"
#include "backend/bridge/ddl.h"
#include "backend/storage/database.h"

#include <vector>

namespace peloton {
namespace test {

#define COLOR_RED     "\x1b[31m"
#define COLOR_GREEN   "\x1b[32m"
#define COLOR_YELLOW  "\x1b[33m"
#define COLOR_BLUE    "\x1b[34m"
#define COLOR_MAGENTA "\x1b[35m"
#define COLOR_CYAN    "\x1b[36m"
#define COLOR_RESET   "\x1b[0m"

//===--------------------------------------------------------------------===//
// CreateTable TEST 
//===--------------------------------------------------------------------===//

void BridgeTest::DDL_CreateTable_TEST() {

  DDL_CreateTable_TEST_INVALID_OID();
  DDL_CreateTable_TEST_COLUMNS();
  DDL_CreateTable_TEST_COLUMN_CONSTRAINTS();

}
void BridgeTest::DDL_COMPREHENSIVE_TEST(){

}

void BridgeTest::DDL_CreateTable_TEST_INVALID_OID() {

  // ColumnInfo
  std::vector<catalog::ColumnInfo> column_infos;

  std::string table_name = "test_table_invalid_oid";
  oid_t table_oid = INVALID_OID;

  // Create a table
  bool status = bridge::DDL::CreateTable( table_oid, table_name, column_infos );
  assert( status == false );
}

void BridgeTest::DDL_CreateTable_TEST_COLUMNS() {
 
  storage::Database* db = storage::Database::GetDatabaseById( GetCurrentDatabaseOid() );

  // ColumnInfo
  std::vector<catalog::ColumnInfo> column_infos;

  catalog::ColumnInfo *column_info1 = new catalog::ColumnInfo( VALUE_TYPE_INTEGER, 4, "id"); 
  catalog::ColumnInfo *column_info2 = new catalog::ColumnInfo( VALUE_TYPE_VARCHAR, 68, "name"); 
  catalog::ColumnInfo *column_info3 = new catalog::ColumnInfo( VALUE_TYPE_TIMESTAMP, 8, "time"); 
  catalog::ColumnInfo *column_info4 = new catalog::ColumnInfo( VALUE_TYPE_DOUBLE, 8, "salary"); 

  column_infos.push_back(*column_info1);
  column_infos.push_back(*column_info2);
  column_infos.push_back(*column_info3);
  column_infos.push_back(*column_info4);

  std::string table_name = "test_table_basic_columns";
  oid_t table_oid = 20001;

  // Create a table
  bool status = bridge::DDL::CreateTable( table_oid, table_name, column_infos );
  assert ( status );

  storage::DataTable* table = db->GetTableById(table_oid);

  // Table name and oid
  assert( strcmp( (table->GetName()).c_str(), table_name.c_str()) == 0 );
  assert( table->GetId() == table_oid );

  // Schema
  catalog::Schema* schema = table->GetSchema();

  // The first column
  catalog::ColumnInfo column = schema->GetColumnInfo(0);

  // Column name, offset, length, type
  assert( strcmp( column.GetName().c_str(), "id" ) == 0 );
  assert( column.GetOffset() == 0 );
  assert( column.GetLength() == 4 );
  assert( column.GetType() == VALUE_TYPE_INTEGER );

  // The second column
  column = schema->GetColumnInfo(1);

  // Column name, offset, length, type
  assert( strcmp( column.GetName().c_str(), "name" ) == 0 );
  assert( column.GetOffset() == 4 );
  assert( column.GetLength() == 68 );
  assert( column.GetType() == VALUE_TYPE_VARCHAR );

  // The third column
  column = schema->GetColumnInfo(2);

  // Column name, offset, length, type
  assert( strcmp( column.GetName().c_str(), "time" ) == 0 );
  assert( column.GetOffset() == 72 );
  assert( column.GetLength() == 8 );
  assert( column.GetType() == VALUE_TYPE_TIMESTAMP );

  // The fourth column
  column = schema->GetColumnInfo(3);

  // Column name, offset, length, type
  assert( strcmp( column.GetName().c_str(), "salary" ) == 0 );
  assert( column.GetOffset() == 80 );
  assert( column.GetLength() == 8 );
  assert( column.GetType() == VALUE_TYPE_DOUBLE );

}

// Create a table and Column-level constraints
// such as primary key, unique, and reference table 
void BridgeTest::DDL_CreateTable_TEST_COLUMN_CONSTRAINTS() {

  storage::Database* db = storage::Database::GetDatabaseById( GetCurrentDatabaseOid() );

  // constraint
  std::vector<catalog::Constraint> constraint_infos;

  catalog::Constraint *notnull_constraint = new catalog::Constraint( CONSTRAINT_TYPE_NOTNULL );
  catalog::Constraint *primary_key_constraint = new catalog::Constraint( CONSTRAINT_TYPE_PRIMARY, "THIS_IS_PRIMARY_KEY_CONSTRAINT" );
  catalog::Constraint *unique_constraint = new catalog::Constraint( CONSTRAINT_TYPE_UNIQUE, "THIS_IS_UNIQUE_CONSTRAINT" );
  catalog::Constraint *foreign_constraint = new catalog::Constraint( CONSTRAINT_TYPE_FOREIGN, "THIS_IS_FOREIGN_CONSTRAINT" );


  // ColumnInfo
  std::vector<catalog::ColumnInfo> column_infos;

  constraint_infos.push_back( *notnull_constraint );
  catalog::ColumnInfo *column_info1 = new catalog::ColumnInfo( VALUE_TYPE_INTEGER, 4, "id", constraint_infos); 
  constraint_infos.clear();

  constraint_infos.push_back( *primary_key_constraint );
  catalog::ColumnInfo *column_info2 = new catalog::ColumnInfo( VALUE_TYPE_VARCHAR, 68, "name", constraint_infos); 
  constraint_infos.clear();

  constraint_infos.push_back( *unique_constraint );
  catalog::ColumnInfo *column_info3 = new catalog::ColumnInfo( VALUE_TYPE_TIMESTAMP, 8, "time", constraint_infos); 
  constraint_infos.clear();

  constraint_infos.push_back( *foreign_constraint );
  catalog::ColumnInfo *column_info4 = new catalog::ColumnInfo( VALUE_TYPE_DOUBLE, 8, "salary", constraint_infos); 
  constraint_infos.clear();

  column_infos.push_back(*column_info1);
  column_infos.push_back(*column_info2);
  column_infos.push_back(*column_info3);
  column_infos.push_back(*column_info4);

  std::string table_name = "test_table_scenario_1";
  oid_t table_oid = 20003;

  // Create a table
  bool status = bridge::DDL::CreateTable( table_oid, table_name, column_infos );
  assert ( status );

  storage::DataTable* table = db->GetTableById(table_oid);

  // Table name and oid
  assert( strcmp( (table->GetName()).c_str(), table_name.c_str()) == 0 );
  assert( table->GetId() == table_oid );

  // Schema
  catalog::Schema* schema = table->GetSchema();

  // The first column
  catalog::ColumnInfo column = schema->GetColumnInfo(0);

  // Column name, offset, length, type, constraint not null
  assert( strcmp( column.GetName().c_str(), "id" ) == 0 );
  assert( column.GetOffset() == 0 );
  assert( column.GetLength() == 4 );
  assert( column.GetType() == VALUE_TYPE_INTEGER );

  constraint_infos.clear();
  constraint_infos = column.GetConstraints();
  assert( constraint_infos.size() == 1 );
  ConstraintType type = constraint_infos[0].GetType();
  assert( type == CONSTRAINT_TYPE_NOTNULL );

  // The second column
  column = schema->GetColumnInfo(1);

  // Column name, offset, length, type, constraint not null
  assert( strcmp( column.GetName().c_str(), "name" ) == 0 );
  assert( column.GetOffset() == 4 );
  assert( column.GetLength() == 68 );
  assert( column.GetType() == VALUE_TYPE_VARCHAR );

  constraint_infos.clear();
  constraint_infos = column.GetConstraints();
  assert( constraint_infos.size() == 1 );
  type = constraint_infos[0].GetType();
  assert( type == CONSTRAINT_TYPE_PRIMARY );
  std::string name = constraint_infos[0].GetName();
  assert( strcmp( name.c_str(), "THIS_IS_PRIMARY_KEY_CONSTRAINT") == 0 );

  // The third column
  column = schema->GetColumnInfo(2);

  // Column name, offset, length, type, constraint not null
  assert( strcmp( column.GetName().c_str(), "time" ) == 0 );
  assert( column.GetOffset() == 72 );
  assert( column.GetLength() == 8 );
  assert( column.GetType() == VALUE_TYPE_TIMESTAMP );

  constraint_infos.clear();
  constraint_infos = column.GetConstraints();
  assert( constraint_infos.size() == 1 );
  type = constraint_infos[0].GetType();
  assert( type == CONSTRAINT_TYPE_UNIQUE );
  name = constraint_infos[0].GetName();
  assert( strcmp( name.c_str(), "THIS_IS_UNIQUE_CONSTRAINT") == 0 );


  // The fourth column
  column = schema->GetColumnInfo(3);

  // Column name, offset, length, type, constraint not null
  assert( strcmp( column.GetName().c_str(), "salary" ) == 0 );
  assert( column.GetOffset() == 80 );
  assert( column.GetLength() == 8 );
  assert( column.GetType() == VALUE_TYPE_DOUBLE );

  constraint_infos.clear();
  constraint_infos = column.GetConstraints();
  assert( constraint_infos.size() == 1 );
  type = constraint_infos[0].GetType();
  assert( type == CONSTRAINT_TYPE_FOREIGN );
  name = constraint_infos[0].GetName();
  assert( strcmp( name.c_str(), "THIS_IS_FOREIGN_CONSTRAINT") == 0 );


  std::vector<std::string> key_column_names;
  key_column_names.push_back("name");

  // Create a primary key index  and add it to the table
  bridge::IndexInfo* index_info = new bridge::IndexInfo( "THIS_IS_PRIMARY_KEY_CONSTRAINT",
                                                         20004,
                                                         table_name, 
                                                         INDEX_METHOD_TYPE_BTREE_MULTIMAP,
                                                         INDEX_TYPE_PRIMARY_KEY,
                                                         true,
                                                         key_column_names);

  status = bridge::DDL::CreateIndex( *index_info );
  assert( status );

  //index::Index *index = table->GetIndex( 20004 );

  // CreateReferecen and add it to the table


}

void BridgeTest::RunTests() {
  printf( COLOR_GREEN "::Start %s" COLOR_RESET "\n", __func__ );
  DDL_CreateTable_TEST();
  DDL_COMPREHENSIVE_TEST();
  printf( COLOR_GREEN "::End %s" COLOR_RESET "\n", __func__ );
}

void BridgeTest::TestCatalog() {
  GetDatabaseList();
}

} // End test namespace
} // End peloton namespace

