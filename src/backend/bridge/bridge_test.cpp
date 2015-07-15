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

void BridgeTest::DDL_CreateTable_TEST() {

  DDL_CreateTable_TEST_INVALID_OID();
  DDL_CreateTable_TEST_COLUMNS();
  DDL_CreateTable_TEST_CONSTRAINTS();

}
void BridgeTest::DDL_CreateTable_TEST_INVALID_OID() {

  // ColumnInfo
  std::vector<catalog::Column> column_infos;

  bool status = bridge::DDL::CreateTable( INVALID_OID, "test_table_invalid_oid", column_infos );
  assert( status == false );
  printf("%s has been finished successfully\n", __func__ );
}

void BridgeTest::DDL_CreateTable_TEST_COLUMNS() {
 
  storage::Database* db = storage::Database::GetDatabaseById( GetCurrentDatabaseOid() );

  // ColumnInfo
  std::vector<catalog::Column> column_infos;

  catalog::Column column_info1( VALUE_TYPE_INTEGER, 4, "id");
  catalog::Column column_info2( VALUE_TYPE_VARCHAR, 68, "name");
  catalog::Column column_info3( VALUE_TYPE_TIMESTAMP, 8, "time");
  catalog::Column column_info4( VALUE_TYPE_DOUBLE, 8, "salary");

  column_infos.push_back(column_info1);
  column_infos.push_back(column_info2);
  column_infos.push_back(column_info3);
  column_infos.push_back(column_info4);

  bool status = bridge::DDL::CreateTable( 20000, "test_table_basic_column", column_infos );
  assert( status );

  storage::DataTable* table = db->GetTableById(20000);

  // Table name and oid
  assert( strcmp( (table->GetName()).c_str(), "test_table_basic_column") == 0 );
  assert( table->GetId() == 20000 );

  // Schema
  catalog::Schema* schema = table->GetSchema();

  // The first column
  catalog::Column column = schema->GetColumn(0);

  // Column name, offset, length, type
  assert( strcmp( column.GetName().c_str(), "id" ) == 0 );
  assert( column.GetOffset() == 0 );
  assert( column.GetLength() == 4 );
  assert( column.GetType() == VALUE_TYPE_INTEGER );

  // The second column
  column = schema->GetColumn(1);

  // Column name, offset, length, type
  assert( strcmp( column.GetName().c_str(), "name" ) == 0 );
  assert( column.GetOffset() == 4 );
  assert( column.GetLength() == 68 );
  assert( column.GetType() == VALUE_TYPE_VARCHAR );

  // The third column
  column = schema->GetColumn(2);

  // Column name, offset, length, type
  assert( strcmp( column.GetName().c_str(), "time" ) == 0 );
  assert( column.GetOffset() == 72 );
  assert( column.GetLength() == 8 );
  assert( column.GetType() == VALUE_TYPE_TIMESTAMP );

  // The fourth column
  column = schema->GetColumn(3);

  // Column name, offset, length, type
  assert( strcmp( column.GetName().c_str(), "salary" ) == 0 );
  assert( column.GetOffset() == 80 );
  assert( column.GetLength() == 8 );
  assert( column.GetType() == VALUE_TYPE_DOUBLE );

  printf("%s has been finished successfully\n", __func__ );
}

void BridgeTest::DDL_CreateTable_TEST_CONSTRAINTS() {

  storage::Database* db = storage::Database::GetDatabaseById( GetCurrentDatabaseOid() );

  // constraints
  std::vector<catalog::Constraint> constraints;

  // constraint
  catalog::Constraint notnull_constraint( CONSTRAINT_TYPE_NOTNULL );
  catalog::Constraint primary_key_constraint( CONSTRAINT_TYPE_PRIMARY, "THIS_IS_PRIMARY_KEY_CONSTRAINT" );
  catalog::Constraint unique_constraint( CONSTRAINT_TYPE_UNIQUE, "THIS_IS_UNIQUE_CONSTRAINT" );
  catalog::Constraint foreign_constraint( CONSTRAINT_TYPE_FOREIGN, "THIS_IS_FOREIGN_CONSTRAINT" );


  // ColumnInfo
  std::vector<catalog::Column> column_infos;

  catalog::Column column_info1( VALUE_TYPE_INTEGER, 4, "id");
  column_info1.AddConstraint(notnull_constraint);

  catalog::Column column_info2( VALUE_TYPE_VARCHAR, 68, "name");
  column_info2.AddConstraint(primary_key_constraint);

  catalog::Column column_info3( VALUE_TYPE_TIMESTAMP, 8, "time");
  column_info3.AddConstraint(unique_constraint);

  catalog::Column column_info4( VALUE_TYPE_DOUBLE, 8, "salary");
  column_info4.AddConstraint(foreign_constraint);

  column_infos.push_back(column_info1);
  column_infos.push_back(column_info2);
  column_infos.push_back(column_info3);
  column_infos.push_back(column_info4);

  // Create a table
  bool status = bridge::DDL::CreateTable( 20001, "test_table_notnull_constraint", column_infos );
  assert ( status );

  storage::DataTable* table = db->GetTableById(20001);

  // Table name and oid
  assert( strcmp( (table->GetName()).c_str(), "test_table_constraints") == 0 );
  assert( table->GetId() == 20001 );

  // Schema
  catalog::Schema* schema = table->GetSchema();

  // The first column
  catalog::Column column = schema->GetColumn(0);

  // Column name, offset, length, type, constraint not null
  assert( strcmp( column.GetName().c_str(), "id" ) == 0 );
  assert( column.GetOffset() == 0 );
  assert( column.GetLength() == 4 );
  assert( column.GetType() == VALUE_TYPE_INTEGER );

  constraints.clear();
  constraints = column.GetConstraints();
  assert( constraints.size() == 1 );
  ConstraintType type = constraints[0].GetType();
  assert( type == CONSTRAINT_TYPE_NOTNULL );


  // The second column
  column = schema->GetColumn(1);

  // Column name, offset, length, type, constraint not null
  assert( strcmp( column.GetName().c_str(), "name" ) == 0 );
  assert( column.GetOffset() == 4 );
  assert( column.GetLength() == 68 );
  assert( column.GetType() == VALUE_TYPE_VARCHAR );

  constraints.clear();
  constraints = column.GetConstraints();
  assert( constraints.size() == 1 );
  type = constraints[0].GetType();
  assert( type == CONSTRAINT_TYPE_PRIMARY );
  std::string name = constraints[0].GetName();
  assert( strcmp( name.c_str(), "THIS_IS_PRIMARY_KEY_CONSTRAINT") == 0 );

  // The third column
  column = schema->GetColumn(2);

  // Column name, offset, length, type, constraint not null
  assert( strcmp( column.GetName().c_str(), "time" ) == 0 );
  assert( column.GetOffset() == 72 );
  assert( column.GetLength() == 8 );
  assert( column.GetType() == VALUE_TYPE_TIMESTAMP );

  constraints.clear();
  constraints = column.GetConstraints();
  assert( constraints.size() == 1 );
  type = constraints[0].GetType();
  assert( type == CONSTRAINT_TYPE_UNIQUE );
  name = constraints[0].GetName();
  assert( strcmp( name.c_str(), "THIS_IS_UNIQUE_CONSTRAINT") == 0 );


  // The fourth column
  column = schema->GetColumn(3);

  // Column name, offset, length, type, constraint not null
  assert( strcmp( column.GetName().c_str(), "salary" ) == 0 );
  assert( column.GetOffset() == 80 );
  assert( column.GetLength() == 8 );
  assert( column.GetType() == VALUE_TYPE_DOUBLE );

  constraints.clear();
  constraints = column.GetConstraints();
  assert( constraints.size() == 1 );
  type = constraints[0].GetType();
  assert( type == CONSTRAINT_TYPE_FOREIGN );
  name = constraints[0].GetName();
  assert( strcmp( name.c_str(), "THIS_IS_FOREIGN_CONSTRAINT") == 0 );

  printf("%s has been finished successfully\n", __func__ );

}

void BridgeTest::RunTests() {
  DDL_CreateTable_TEST();
}

void BridgeTest::TestCatalog() {
  GetDatabaseList();
}

} // End test namespace
} // End peloton namespace

