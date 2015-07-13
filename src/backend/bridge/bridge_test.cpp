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
#include "backend/catalog/schema.h"

#include <vector>

namespace peloton {
namespace test {

void BridgeTest::RunTests() {

  //===--------------------------------------------------------------------===//
  // CreateTable TEST 
  //===--------------------------------------------------------------------===//
  {
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
    assert ( status );
  }

  //===--------------------------------------------------------------------===//
  // CreateTable TEST with Constraint
  //===--------------------------------------------------------------------===//
  {
    std::vector<catalog::ColumnInfo> column_infos;
    std::vector<catalog::Constraint> constraint_infos;

    catalog::Constraint *notnull_constraint = new catalog::Constraint( CONSTRAINT_TYPE_NOTNULL );
    catalog::Constraint *primary_constraint = new catalog::Constraint( CONSTRAINT_TYPE_PRIMARY );
    catalog::Constraint *unique_constraint = new catalog::Constraint( CONSTRAINT_TYPE_UNIQUE );
    catalog::Constraint *foreign_constraint = new catalog::Constraint( CONSTRAINT_TYPE_FOREIGN );

    constraint_infos.push_back( *notnull_constraint );
    constraint_infos.push_back( *primary_constraint );
    constraint_infos.push_back( *unique_constraint );
    constraint_infos.push_back( *foreign_constraint );

    catalog::ColumnInfo *column_info1 = new catalog::ColumnInfo( VALUE_TYPE_INTEGER, 4, "id", constraint_infos); 
    catalog::ColumnInfo *column_info2 = new catalog::ColumnInfo( VALUE_TYPE_VARCHAR, 68, "name", constraint_infos); 
    catalog::ColumnInfo *column_info3 = new catalog::ColumnInfo( VALUE_TYPE_TIMESTAMP, 8, "time", constraint_infos); 
    catalog::ColumnInfo *column_info4 = new catalog::ColumnInfo( VALUE_TYPE_DOUBLE, 8, "salary", constraint_infos); 

    column_infos.push_back(*column_info1);
    column_infos.push_back(*column_info2);
    column_infos.push_back(*column_info3);
    column_infos.push_back(*column_info4);

    bool status = peloton::bridge::DDL::CreateTable( INVALID_OID, "test_table2", column_infos );
    assert ( status );
  }




  TestCatalog();
}

void BridgeTest::TestCatalog() {
  GetDatabaseList();
}

} // End test namespace
} // End peloton namespace

