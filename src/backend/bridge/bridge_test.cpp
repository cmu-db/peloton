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

  //DDL_CreateTable_TEST_INVALID_OID();

  DDL_CreateTable_TEST_BASIC_COLUMNS();

}
void BridgeTest::DDL_CreateTable_TEST_INVALID_OID() {

  // ColumnInfo vector
  std::vector<catalog::ColumnInfo> column_infos;

  bool status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table_invalid_oid", column_infos );
  assert( status == false );
}

void BridgeTest::DDL_CreateTable_TEST_BASIC_COLUMNS() {
 
  peloton::storage::Database* db = peloton::storage::Database::GetDatabaseById( GetCurrentDatabaseOid() );

  // ColumnInfo vector
  std::vector<catalog::ColumnInfo> column_infos;

  catalog::ColumnInfo *column_info1 = new catalog::ColumnInfo( VALUE_TYPE_INTEGER, 4, "id"); 
  catalog::ColumnInfo *column_info2 = new catalog::ColumnInfo( VALUE_TYPE_VARCHAR, 68, "name"); 
  catalog::ColumnInfo *column_info3 = new catalog::ColumnInfo( VALUE_TYPE_TIMESTAMP, 8, "time"); 
  catalog::ColumnInfo *column_info4 = new catalog::ColumnInfo( VALUE_TYPE_DOUBLE, 8, "salary"); 

  column_infos.push_back(*column_info1);
  column_infos.push_back(*column_info2);
  column_infos.push_back(*column_info3);
  column_infos.push_back(*column_info4);

  bool status = peloton::bridge::DDL::CreateTable( 20000, "test_table_basic_column", column_infos );
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

  // The second column
  column = schema->GetColumnInfo(2);

  // Column name, offset, length, type
  assert( strcmp( column.GetName().c_str(), "time" ) == 0 );
  assert( column.GetOffset() == 72 );
  assert( column.GetLength() == 8 );
  assert( column.GetType() == VALUE_TYPE_TIMESTAMP );

  // The second column
  column = schema->GetColumnInfo(3);

  // Column name, offset, length, type
  assert( strcmp( column.GetName().c_str(), "salary" ) == 0 );
  assert( column.GetOffset() == 80 );
  assert( column.GetLength() == 8 );
  assert( column.GetType() == VALUE_TYPE_DOUBLE );

}

/*
void BridgeTest::DDL_CreateTable_TEST_2() {

  //===--------------------------------------------------------------------===//
  // CreateTable TEST 
  //===--------------------------------------------------------------------===//
  {
    // COLUMN VECTOR
    std::vector<catalog::ColumnInfo> column_infos;

    catalog::ColumnInfo *column_info1 = new catalog::ColumnInfo( VALUE_TYPE_INTEGER, 4, "id"); 
    catalog::ColumnInfo *column_info2 = new catalog::ColumnInfo( VALUE_TYPE_VARCHAR, 68, "name"); 
    catalog::ColumnInfo *column_info3 = new catalog::ColumnInfo( VALUE_TYPE_TIMESTAMP, 8, "time"); 
    catalog::ColumnInfo *column_info4 = new catalog::ColumnInfo( VALUE_TYPE_DOUBLE, 8, "salary"); 

    column_infos.push_back(*column_info1);
    column_infos.push_back(*column_info2);
    column_infos.push_back(*column_info3);
    column_infos.push_back(*column_info4);

    bool status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table1", column_infos );
    //bool status = peloton::bridge::DDL::CreateTable( 33, "test_table1", column_infos );
    assert(status );

    // CONSTRAINT VECTOR
    std::vector<catalog::Constraint> constraint_infos;

    // TEST :: NOT NULL
    // Create constraints
    catalog::Constraint *notnull_constraint = new catalog::Constraint( CONSTRAINT_TYPE_NOTNULL );
    constraint_infos.push_back( *notnull_constraint );

    // Attach it to the column
    catalog::ColumnInfo *column_info5 = new catalog::ColumnInfo( VALUE_TYPE_INTEGER, 4, "id", constraint_infos); 
    column_infos.push_back(*column_info5);

    // Create a table
    status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table2", column_infos );
    assert ( status );


    // TEST :: PRIMARY KEY & UNIQUE INDEX
    // Create constraints
    catalog::Constraint *primary_constraint = new catalog::Constraint( CONSTRAINT_TYPE_PRIMARY );
    catalog::Constraint *unique_constraint = new catalog::Constraint( CONSTRAINT_TYPE_UNIQUE );
    constraint_infos.push_back( *primary_constraint );
    constraint_infos.push_back( *unique_constraint );

    // Attach them to the column
    catalog::ColumnInfo *column_info6 = new catalog::ColumnInfo( VALUE_TYPE_VARCHAR, 68, "name", constraint_infos); 
    catalog::ColumnInfo *column_info7 = new catalog::ColumnInfo( VALUE_TYPE_TIMESTAMP, 8, "time", constraint_infos); 
    column_infos.push_back(*column_info6);
    column_infos.push_back(*column_info7);

    // Create a table
    status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table3", column_infos );
    assert ( status );


    // TEST :: FOREIGN KEY 
    // Create a constraint
    catalog::Constraint *foreign_constraint = new catalog::Constraint( CONSTRAINT_TYPE_FOREIGN );
    constraint_infos.push_back( *foreign_constraint );

    // Attach it to the column
    catalog::ColumnInfo *column_info8 = new catalog::ColumnInfo( VALUE_TYPE_DOUBLE, 8, "salary", constraint_infos); 
    column_infos.push_back(*column_info8);

    // Create a table
    status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table4", column_infos );
    assert ( status );
  }

}
void BridgeTest::DDL_CreateTable_TEST_2() {

  //===--------------------------------------------------------------------===//
  // CreateTable TEST 
  //===--------------------------------------------------------------------===//
  {
    // COLUMN VECTOR
    std::vector<catalog::ColumnInfo> column_infos;

    catalog::ColumnInfo *column_info1 = new catalog::ColumnInfo( VALUE_TYPE_INTEGER, 4, "id"); 
    catalog::ColumnInfo *column_info2 = new catalog::ColumnInfo( VALUE_TYPE_VARCHAR, 68, "name"); 
    catalog::ColumnInfo *column_info3 = new catalog::ColumnInfo( VALUE_TYPE_TIMESTAMP, 8, "time"); 
    catalog::ColumnInfo *column_info4 = new catalog::ColumnInfo( VALUE_TYPE_DOUBLE, 8, "salary"); 

    column_infos.push_back(*column_info1);
    column_infos.push_back(*column_info2);
    column_infos.push_back(*column_info3);
    column_infos.push_back(*column_info4);

    bool status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table1", column_infos );
    //bool status = peloton::bridge::DDL::CreateTable( 33, "test_table1", column_infos );
    assert(status );

    // CONSTRAINT VECTOR
    std::vector<catalog::Constraint> constraint_infos;

    // TEST :: NOT NULL
    // Create constraints
    catalog::Constraint *notnull_constraint = new catalog::Constraint( CONSTRAINT_TYPE_NOTNULL );
    constraint_infos.push_back( *notnull_constraint );

    // Attach it to the column
    catalog::ColumnInfo *column_info5 = new catalog::ColumnInfo( VALUE_TYPE_INTEGER, 4, "id", constraint_infos); 
    column_infos.push_back(*column_info5);

    // Create a table
    status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table2", column_infos );
    assert ( status );


    // TEST :: PRIMARY KEY & UNIQUE INDEX
    // Create constraints
    catalog::Constraint *primary_constraint = new catalog::Constraint( CONSTRAINT_TYPE_PRIMARY );
    catalog::Constraint *unique_constraint = new catalog::Constraint( CONSTRAINT_TYPE_UNIQUE );
    constraint_infos.push_back( *primary_constraint );
    constraint_infos.push_back( *unique_constraint );

    // Attach them to the column
    catalog::ColumnInfo *column_info6 = new catalog::ColumnInfo( VALUE_TYPE_VARCHAR, 68, "name", constraint_infos); 
    catalog::ColumnInfo *column_info7 = new catalog::ColumnInfo( VALUE_TYPE_TIMESTAMP, 8, "time", constraint_infos); 
    column_infos.push_back(*column_info6);
    column_infos.push_back(*column_info7);

    // Create a table
    status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table3", column_infos );
    assert ( status );


    // TEST :: FOREIGN KEY 
    // Create a constraint
    catalog::Constraint *foreign_constraint = new catalog::Constraint( CONSTRAINT_TYPE_FOREIGN );
    constraint_infos.push_back( *foreign_constraint );

    // Attach it to the column
    catalog::ColumnInfo *column_info8 = new catalog::ColumnInfo( VALUE_TYPE_DOUBLE, 8, "salary", constraint_infos); 
    column_infos.push_back(*column_info8);

    // Create a table
    status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table4", column_infos );
    assert ( status );
  }

}
void BridgeTest::DDL_CreateTable_TEST_2() {

  //===--------------------------------------------------------------------===//
  // CreateTable TEST 
  //===--------------------------------------------------------------------===//
  {
    // COLUMN VECTOR
    std::vector<catalog::ColumnInfo> column_infos;

    catalog::ColumnInfo *column_info1 = new catalog::ColumnInfo( VALUE_TYPE_INTEGER, 4, "id"); 
    catalog::ColumnInfo *column_info2 = new catalog::ColumnInfo( VALUE_TYPE_VARCHAR, 68, "name"); 
    catalog::ColumnInfo *column_info3 = new catalog::ColumnInfo( VALUE_TYPE_TIMESTAMP, 8, "time"); 
    catalog::ColumnInfo *column_info4 = new catalog::ColumnInfo( VALUE_TYPE_DOUBLE, 8, "salary"); 

    column_infos.push_back(*column_info1);
    column_infos.push_back(*column_info2);
    column_infos.push_back(*column_info3);
    column_infos.push_back(*column_info4);

    bool status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table1", column_infos );
    //bool status = peloton::bridge::DDL::CreateTable( 33, "test_table1", column_infos );
    assert(status );

    // CONSTRAINT VECTOR
    std::vector<catalog::Constraint> constraint_infos;

    // TEST :: NOT NULL
    // Create constraints
    catalog::Constraint *notnull_constraint = new catalog::Constraint( CONSTRAINT_TYPE_NOTNULL );
    constraint_infos.push_back( *notnull_constraint );

    // Attach it to the column
    catalog::ColumnInfo *column_info5 = new catalog::ColumnInfo( VALUE_TYPE_INTEGER, 4, "id", constraint_infos); 
    column_infos.push_back(*column_info5);

    // Create a table
    status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table2", column_infos );
    assert ( status );


    // TEST :: PRIMARY KEY & UNIQUE INDEX
    // Create constraints
    catalog::Constraint *primary_constraint = new catalog::Constraint( CONSTRAINT_TYPE_PRIMARY );
    catalog::Constraint *unique_constraint = new catalog::Constraint( CONSTRAINT_TYPE_UNIQUE );
    constraint_infos.push_back( *primary_constraint );
    constraint_infos.push_back( *unique_constraint );

    // Attach them to the column
    catalog::ColumnInfo *column_info6 = new catalog::ColumnInfo( VALUE_TYPE_VARCHAR, 68, "name", constraint_infos); 
    catalog::ColumnInfo *column_info7 = new catalog::ColumnInfo( VALUE_TYPE_TIMESTAMP, 8, "time", constraint_infos); 
    column_infos.push_back(*column_info6);
    column_infos.push_back(*column_info7);

    // Create a table
    status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table3", column_infos );
    assert ( status );


    // TEST :: FOREIGN KEY 
    // Create a constraint
    catalog::Constraint *foreign_constraint = new catalog::Constraint( CONSTRAINT_TYPE_FOREIGN );
    constraint_infos.push_back( *foreign_constraint );

    // Attach it to the column
    catalog::ColumnInfo *column_info8 = new catalog::ColumnInfo( VALUE_TYPE_DOUBLE, 8, "salary", constraint_infos); 
    column_infos.push_back(*column_info8);

    // Create a table
    status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table4", column_infos );
    assert ( status );
  }

}
*/

void BridgeTest::RunTests() {
  DDL_CreateTable();
}

void BridgeTest::TestCatalog() {
  GetDatabaseList();
}

} // End test namespace
} // End peloton namespace

