//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bridge_test_helper.cpp
//
// Identification: src/backend/bridge/ddl/tests/bridge_test_helper.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "bridge_test.h"

#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_table.h"
#include "backend/bridge/ddl/ddl_index.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/foreign_key.h"
#include "backend/storage/database.h"

#include "tcop/tcopprot.h"
#include "parser/parse_utilcmd.h"
#include "catalog/pg_class.h"
#include "commands/tablecmds.h"

namespace peloton {
namespace bridge {

/**
 * @brief Create a simple Column just for convenience
 * @return the vector of Column
 */
std::vector<catalog::Column> BridgeTest::CreateSimpleColumns() {
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

/**
 * @brief Compare the given column with given information
 * @param column column what to check
 * @param column_name column_name to be compared with column's name
 * @param length length to be compared with column's length
 * @param type valueType to be compared with column's valueType
 * @return the true if we pass all
 */
bool BridgeTest::CheckColumn(catalog::Column &column, std::string column_name,
                             int length, ValueType type) {
  assert(strcmp(column.GetName().c_str(), column_name.c_str()) == 0);
  assert(column.GetLength() == length);
  assert(column.GetType() == type);

  return true;
}

/**
 * @brief Compare the given column with given information
 * @param column column what to check
 * @param constraint_type ConstraintType to be compared with column's constraint
 * type
 * @param constraint_name constraint_name to be compared with columns's
 * constraint name
 * @param constraint_count constraint_count to be compared with columns's
 * constraint count
 * @return the true if we pass all
 */
bool BridgeTest::CheckColumnWithConstraint(catalog::Column &column,
                                           ConstraintType constraint_type,
                                           std::string constraint_name,
                                           unsigned int constraint_count,
                                           int foreign_key_offset) {
  std::vector<catalog::Constraint> constraint_infos = column.GetConstraints();
  assert(constraint_infos[0].GetType() == constraint_type);

  if (constraint_infos[0].GetName().size() > 0 && constraint_name.size() > 0)
    assert(strcmp(constraint_infos[0].GetName().c_str(),
                  constraint_name.c_str()) == 0);

  assert(constraint_infos.size() == constraint_count);
  if (constraint_type == CONSTRAINT_TYPE_FOREIGN)
    assert(constraint_infos[0].GetForeignKeyListOffset() == foreign_key_offset);

  return true;
}

/**
 * @brief Compare the given index with given information
 * @param index index what to check
 * @param index_name index_name to be compared with index's name
 * @param column_count column count to be compared with index's column count
 * @param method_type method type to be compared with index's method type
 * @param constraint_type constraint type to be compared with index's constraint
 * type
 * @param unique unique key to be compared with index's unique
 * @return the true if we pass all
 */
bool BridgeTest::CheckIndex(index::Index *index, std::string index_name,
                            oid_t column_count, IndexType method_type,
                            IndexConstraintType constraint_type, bool unique) {
  assert(strcmp(index->GetName().c_str(), index_name.c_str()) == 0);
  assert(index->GetColumnCount() == column_count);
  assert(index->GetIndexMethodType() == method_type);
  assert(index->GetIndexType() == constraint_type);
  assert(index->HasUniqueKeys() == unique);

  return true;
}

/**
 * @brief Compare the given foreign key with given information
 * @param foreign_key foreign key what to check
 * @param pktable_oid pktable oid to be compared with foreign key's oid
 * @param constraint_name constraint name to be compared with foreign key's
 * constraint name
 * @param _pk_column_names to be compared with foreign key's pk_column_names
 * @param _fk_column_names to be compared with foreign key's fk_column_names
 * @param fk_update_action foreign key update action to be compared with foreign
 * key's update action
 * @param fk_delete_action foreign key delete action to be compared with foreign
 * key's delete action
 * @return the true if we pass all
 */
bool BridgeTest::CheckForeignKey(catalog::ForeignKey *foreign_key,
                                 oid_t pktable_oid, std::string constraint_name,
                                 unsigned int pk_column_names_count,
                                 unsigned int fk_column_names_count,
                                 char fk_update_action, char fk_delete_action) {
  assert(foreign_key->GetSinkTableOid() == pktable_oid);
  assert(strcmp((foreign_key->GetConstraintName()).c_str(),
                constraint_name.c_str()) == 0);

  std::vector<std::string> pk_column_names = foreign_key->GetPKColumnNames();
  std::vector<std::string> fk_column_names = foreign_key->GetFKColumnNames();

  assert(pk_column_names.size() == pk_column_names_count);
  assert(fk_column_names.size() == fk_column_names_count);

  assert(foreign_key->GetUpdateAction() == fk_update_action);
  assert(foreign_key->GetDeleteAction() == fk_delete_action);

  return true;
}

/**
 * @brief Create a sample primary key index
 * @params table_name table name new index will belong to
 * @params index_oid index oid
 */
void BridgeTest::CreateSamplePrimaryKeyIndex(std::string table_name,
                                             oid_t index_oid) {
  bool status;
  std::vector<std::string> key_column_names;
  key_column_names.push_back("name");
  IndexInfo *index_info;

  index_info = new IndexInfo(
      table_name + "_pkey", index_oid, table_name, INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, true, key_column_names);

  status = DDLIndex::CreateIndex(*index_info);
  assert(status);
}

/**
 * @brief Create a sample unique key index
 * @params table_name table name new index will belong to
 * @params index_oid index oid
 */
void BridgeTest::CreateSampleUniqueIndex(std::string table_name,
                                         oid_t index_oid) {
  bool status;
  std::vector<std::string> key_column_names;
  key_column_names.push_back("time");
  IndexInfo *index_info;

  index_info = new IndexInfo(table_name + "_key", index_oid, table_name,
                             INDEX_TYPE_BTREE, INDEX_CONSTRAINT_TYPE_UNIQUE,
                             true, key_column_names);

  status = DDLIndex::CreateIndex(*index_info);
  assert(status);
}

/**
 * @brief Create a sample foreign key
 * @params pktable_oid pktable oid
 * @params pktable_name pktable name
 * @params column column
 * @params table_oid table oid that foreign key will belong to
 */
void BridgeTest::CreateSampleForeignKey(oid_t pktable_oid,
                                        std::string pktable_name,
                                        std::vector<catalog::Column> &columns,
                                        oid_t table_oid) {
  bool status;
  // Create a sample table that has primary key index
  status = DDLTable::CreateTable(pktable_oid, pktable_name, columns);
  assert(status);

  std::vector<std::string> pk_column_names;
  std::vector<std::string> fk_column_names;
  pk_column_names.push_back("name");
  fk_column_names.push_back("salary");
  std::vector<catalog::ForeignKey> foreign_keys;
  catalog::ForeignKey *foreign_key =
      new catalog::ForeignKey(pktable_oid, pk_column_names, fk_column_names,
                              'r', 'c', "THIS_IS_FOREIGN_CONSTRAINT");
  foreign_keys.push_back(*foreign_key);

  // Current table ----> reference table
  status = DDLTable::SetReferenceTables(foreign_keys, table_oid);
  assert(status);
}

/**
 * @brief Create a table in Postgres
 * @params table_name table name
 * @return table_oid
 */
oid_t BridgeTest::CreateTableInPostgres(std::string table_name) {
  std::string queryString =
      "create table " + table_name +
      "(id int, name char(64), time timestamp, salary double precision);";

  ObjectAddress address;
  List *parsetree_list;
  ListCell *parsetree_item;

  //FIXME Do we need now? 
  //StartTransactionCommand();

  parsetree_list = pg_parse_query(queryString.c_str());
  foreach (parsetree_item, parsetree_list) {
    Node *parsetree = (Node *)lfirst(parsetree_item);

    List *stmts;
    /* Run parse analysis ... */
    stmts = transformCreateStmt((CreateStmt *)parsetree, queryString.c_str());

    /* ... and do it */
    ListCell *l;
    foreach (l, stmts) {
      Node *stmt = (Node *)lfirst(l);

      if (IsA(stmt, CreateStmt)) {
        /* Create the table itself */
        address = DefineRelation((CreateStmt *)stmt, RELKIND_RELATION,
                                 InvalidOid, NULL);
      }
    }
  }

  //FIXME Do we need now? 
  //CommitTransactionCommand();

  return address.objectId;
}

/**
 * @brief Drop the table in Postgres
 * @params table_name table name
 * @return true if we drop the table
 */
bool BridgeTest::DropTableInPostgres(std::string table_name) {
  //FIXME Do we need now? 
  //StartTransactionCommand();

  std::string queryString = "drop table " + table_name + ";";

  List *parsetree_list;
  ListCell *parsetree_item;

  parsetree_list = pg_parse_query(queryString.c_str());
  foreach (parsetree_item, parsetree_list) {
    Node *parsetree = (Node *)lfirst(parsetree_item);

    DropStmt *stmt = (DropStmt *)parsetree;

    // Since Postgres requires many functions to remove the relation, sometimes
    // it incurs event cache look up problems. This wrapper function simply
    // drop the table from Postgres.
    PelotonRemoveRelations(stmt);
  }

  //FIXME Do we need now? 
  //CommitTransactionCommand();

  return true;
}

}  // End test namespace
}  // End peloton namespace
