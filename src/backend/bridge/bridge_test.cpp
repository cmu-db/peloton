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

//===--------------------------------------------------------------------===//
// CreateTable TEST 
//===--------------------------------------------------------------------===//

void BridgeTest::DDL_CreateTable() {

  DDL_CreateTable_TEST_INVALID_OID();

  DDL_CreateTable_TEST_BASIC_COLUMNS();

  DDL_CreateTable_TEST_NOTNULL_CONSTRAINT();

}
void BridgeTest::DDL_CreateTable_TEST_INVALID_OID() {

  // ColumnInfo
  std::vector<catalog::ColumnInfo> column_infos;

  bool status = bridge::DDL::CreateTable( INVALID_OID, "test_table_invalid_oid", column_infos );
  assert( status == false );
  printf("%s has been finished successfully\n", __func__ );
}

void BridgeTest::DDL_CreateTable_TEST_BASIC_COLUMNS() {
 
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

  bool status = bridge::DDL::CreateTable( 20000, "test_table_basic_column", column_infos );
  assert( status );

  storage::DataTable* table = db->GetTableById(20000);

  // Table name and oid
  assert( strcmp( (table->GetName()).c_str(), "test_table_basic_column") == 0 );
  assert( table->GetId() == 20000 );

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

  printf("%s has been finished successfully\n", __func__ );
}

void BridgeTest::DDL_CreateTable_TEST_NOTNULL_CONSTRAINT() {

  storage::Database* db = storage::Database::GetDatabaseById( GetCurrentDatabaseOid() );

  // constraint
  std::vector<catalog::Constraint> constraint_infos;

  // NOT NULL
  // Create constraints
  catalog::Constraint *notnull_constraint = new catalog::Constraint( CONSTRAINT_TYPE_NOTNULL );
  constraint_infos.push_back( *notnull_constraint );

  // ColumnInfo
  std::vector<catalog::ColumnInfo> column_infos;

  catalog::ColumnInfo *column_info1 = new catalog::ColumnInfo( VALUE_TYPE_INTEGER, 4, "id", constraint_infos); 
  catalog::ColumnInfo *column_info2 = new catalog::ColumnInfo( VALUE_TYPE_VARCHAR, 68, "name", constraint_infos); 
  catalog::ColumnInfo *column_info3 = new catalog::ColumnInfo( VALUE_TYPE_TIMESTAMP, 8, "time", constraint_infos); 
  catalog::ColumnInfo *column_info4 = new catalog::ColumnInfo( VALUE_TYPE_DOUBLE, 8, "salary", constraint_infos); 

  column_infos.push_back(*column_info1);
  column_infos.push_back(*column_info2);
  column_infos.push_back(*column_info3);
  column_infos.push_back(*column_info4);

  // Create a table
  bool status = bridge::DDL::CreateTable( 20001, "test_table_notnull_constraint", column_infos );
  assert ( status );

  storage::DataTable* table = db->GetTableById(20001);

  // Table name and oid
  assert( strcmp( (table->GetName()).c_str(), "test_table_notnull_constraint") == 0 );
  assert( table->GetId() == 20001 );

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
  assert( type == CONSTRAINT_TYPE_NOTNULL );

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
  assert( type == CONSTRAINT_TYPE_NOTNULL );

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
  assert( type == CONSTRAINT_TYPE_NOTNULL );

  printf("%s has been finished successfully\n", __func__ );

}

void BridgeTest::RunTests() {
  DDL_CreateTable();
}

void BridgeTest::TestCatalog() {
  GetDatabaseList();
}

} // End test namespace
} // End peloton namespace

