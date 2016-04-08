//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// loader.cpp
//
// Identification: benchmark/tpcc/loader.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>

#include "backend/benchmark/tpcc/tpcc_loader.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"
#include "backend/concurrency/transaction.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/index/index_factory.h"
#include "backend/planner/insert_plan.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

storage::Database* tpcc_database;
storage::DataTable* warehouse_table;
storage::DataTable* district_table;
storage::DataTable* item_table;
storage::DataTable* customer_table;
storage::DataTable* history_table;
storage::DataTable* stock_table;
storage::DataTable* orders_table;
storage::DataTable* new_order_table;
storage::DataTable* order_line_table;

void CreateWarehouseTable() {
  /*
   CREATE TABLE WAREHOUSE (
   W_ID SMALLINT DEFAULT '0' NOT NULL,
   W_NAME VARCHAR(16) DEFAULT NULL,
   W_STREET_1 VARCHAR(32) DEFAULT NULL,
   W_STREET_2 VARCHAR(32) DEFAULT NULL,
   W_CITY VARCHAR(32) DEFAULT NULL,
   W_STATE VARCHAR(2) DEFAULT NULL,
   W_ZIP VARCHAR(9) DEFAULT NULL,
   W_TAX FLOAT DEFAULT NULL,
   W_YTD FLOAT DEFAULT NULL,
   CONSTRAINT W_PK_ARRAY PRIMARY KEY (W_ID)
   );
   */

  // Create schema first
  const bool own_schema = true;
  const bool adapt_table = false;
  const bool is_inlined = true;

  std::vector<catalog::Column> warehouse_columns;

  auto w_id_column = catalog::Column(VALUE_TYPE_SMALLINT, GetTypeSize(VALUE_TYPE_SMALLINT), "W_ID", is_inlined);
  warehouse_columns.push_back(w_id_column);
  auto w_name_column = catalog::Column(VALUE_TYPE_VARCHAR, 16, "W_NAME", is_inlined);
  warehouse_columns.push_back(w_name_column);
  auto w_street_1_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "W_STREET_1", is_inlined);
  warehouse_columns.push_back(w_street_1_column);
  auto w_street_2_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "W_STREET_2", is_inlined);
  warehouse_columns.push_back(w_street_2_column);
  auto w_city_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "W_CITY", is_inlined);
  warehouse_columns.push_back(w_city_column);
  auto w_state_column = catalog::Column(VALUE_TYPE_VARCHAR, 2, "W_STATE", is_inlined);
  warehouse_columns.push_back(w_state_column);
  auto w_zip_column = catalog::Column(VALUE_TYPE_VARCHAR, 9, "W_ZIP", is_inlined);
  warehouse_columns.push_back(w_zip_column);
  auto w_tax_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "W_TAX", is_inlined);
  warehouse_columns.push_back(w_tax_column);
  auto w_ytd_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "W_YTD", is_inlined);
  warehouse_columns.push_back(w_ytd_column);

  catalog::Schema *table_schema = new catalog::Schema(warehouse_columns);
  std::string table_name("WAREHOUSE");

  warehouse_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid,
      warehouse_table_oid,
      table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema,
      adapt_table);

  tpcc_database->AddTable(warehouse_table);

  // Primary index on W_ID
  std::vector<oid_t> key_attrs;

  auto tuple_schema = warehouse_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "warehouse_pkey",
      warehouse_table_pkey_index_oid,
      INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
  warehouse_table->AddIndex(pkey_index);

}

void CreateDistrictTable() {
  /*
   CREATE TABLE DISTRICT (
   D_ID TINYINT DEFAULT '0' NOT NULL,
   D_W_ID SMALLINT DEFAULT '0' NOT NULL REFERENCES WAREHOUSE (W_ID),
   D_NAME VARCHAR(16) DEFAULT NULL,
   D_STREET_1 VARCHAR(32) DEFAULT NULL,
   D_STREET_2 VARCHAR(32) DEFAULT NULL,
   D_CITY VARCHAR(32) DEFAULT NULL,
   D_STATE VARCHAR(2) DEFAULT NULL,
   D_ZIP VARCHAR(9) DEFAULT NULL,
   D_TAX FLOAT DEFAULT NULL,
   D_YTD FLOAT DEFAULT NULL,
   D_NEXT_O_ID INT DEFAULT NULL,
   PRIMARY KEY (D_W_ID,D_ID)
   );
   */

  // Create schema first
  const bool own_schema = true;
  const bool adapt_table = false;
  const bool is_inlined = true;

  std::vector<catalog::Column> district_columns;

  auto d_id_column = catalog::Column(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "D_ID", is_inlined);
  district_columns.push_back(d_id_column);
  auto d_w_id_column = catalog::Column(VALUE_TYPE_SMALLINT, GetTypeSize(VALUE_TYPE_SMALLINT), "D_W_ID", is_inlined);
  district_columns.push_back(d_w_id_column);
  auto d_name_column = catalog::Column(VALUE_TYPE_VARCHAR, 16, "D_NAME", is_inlined);
  district_columns.push_back(d_name_column);
  auto d_street_1_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "D_STREET_1", is_inlined);
  district_columns.push_back(d_street_1_column);
  auto d_street_2_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "D_STREET_2", is_inlined);
  district_columns.push_back(d_street_2_column);
  auto d_city_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "D_CITY", is_inlined);
  district_columns.push_back(d_city_column);
  auto d_state_column = catalog::Column(VALUE_TYPE_VARCHAR, 2, "D_STATE", is_inlined);
  district_columns.push_back(d_state_column);
  auto d_zip_column = catalog::Column(VALUE_TYPE_VARCHAR, 9, "D_ZIP", is_inlined);
  district_columns.push_back(d_zip_column);
  auto d_tax_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "D_TAX", is_inlined);
  district_columns.push_back(d_tax_column);
  auto d_ytd_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "D_YTD", is_inlined);
  district_columns.push_back(d_ytd_column);
  auto d_next_o_id_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "D_NEXT_O_ID", is_inlined);
  district_columns.push_back(d_next_o_id_column);

  catalog::Schema *table_schema = new catalog::Schema(district_columns);
  std::string table_name("DISTRICT");

  district_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid,
      district_table_oid,
      table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema,
      adapt_table);

  tpcc_database->AddTable(district_table);

  // Primary index on D_ID, D_W_ID
  std::vector<oid_t> key_attrs;

  auto tuple_schema = district_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0, 1};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "district_pkey",
      district_table_pkey_index_oid,
      INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
  district_table->AddIndex(pkey_index);

}

void CreateItemTable() {
  /*
   CREATE TABLE ITEM (
   I_ID INTEGER DEFAULT '0' NOT NULL,
   I_IM_ID INTEGER DEFAULT NULL,
   I_NAME VARCHAR(32) DEFAULT NULL,
   I_PRICE FLOAT DEFAULT NULL,
   I_DATA VARCHAR(64) DEFAULT NULL,
   CONSTRAINT I_PK_ARRAY PRIMARY KEY (I_ID)
   );
   */

  // Create schema first
  const bool own_schema = true;
  const bool adapt_table = false;
  const bool is_inlined = true;

  std::vector<catalog::Column> item_columns;

  auto i_id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "I_ID", is_inlined);
  item_columns.push_back(i_id_column);
  auto i_im_id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "I_IM_ID", is_inlined);
  item_columns.push_back(i_im_id_column);
  auto i_name_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "I_NAME", is_inlined);
  item_columns.push_back(i_name_column);
  auto i_price_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "I_PRICE", is_inlined);
  item_columns.push_back(i_price_column);
  auto i_data_column = catalog::Column(VALUE_TYPE_VARCHAR, 64, "I_DATA", is_inlined);
  item_columns.push_back(i_data_column);

  catalog::Schema *table_schema = new catalog::Schema(item_columns);
  std::string table_name("ITEM");

  item_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid,
      item_table_oid,
      table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema,
      adapt_table);

  tpcc_database->AddTable(item_table);

  // Primary index on I_ID
  std::vector<oid_t> key_attrs;

  auto tuple_schema = item_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "item_pkey",
      item_table_pkey_index_oid,
      INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
  item_table->AddIndex(pkey_index);

}

void CreateCustomerTable() {
  /*
     CREATE TABLE CUSTOMER (
     C_ID INTEGER DEFAULT '0' NOT NULL,
     C_D_ID TINYINT DEFAULT '0' NOT NULL,
     C_W_ID SMALLINT DEFAULT '0' NOT NULL,
     C_FIRST VARCHAR(32) DEFAULT NULL,
     C_MIDDLE VARCHAR(2) DEFAULT NULL,
     C_LAST VARCHAR(32) DEFAULT NULL,
     C_STREET_1 VARCHAR(32) DEFAULT NULL,
     C_STREET_2 VARCHAR(32) DEFAULT NULL,
     C_CITY VARCHAR(32) DEFAULT NULL,
     C_STATE VARCHAR(2) DEFAULT NULL,
     C_ZIP VARCHAR(9) DEFAULT NULL,
     C_PHONE VARCHAR(32) DEFAULT NULL,
     C_SINCE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
     C_CREDIT VARCHAR(2) DEFAULT NULL,
     C_CREDIT_LIM FLOAT DEFAULT NULL,
     C_DISCOUNT FLOAT DEFAULT NULL,
     C_BALANCE FLOAT DEFAULT NULL,
     C_YTD_PAYMENT FLOAT DEFAULT NULL,
     C_PAYMENT_CNT INTEGER DEFAULT NULL,
     C_DELIVERY_CNT INTEGER DEFAULT NULL,
     C_DATA VARCHAR(500),
     PRIMARY KEY (C_W_ID,C_D_ID,C_ID),
     UNIQUE (C_W_ID,C_D_ID,C_LAST,C_FIRST),
     CONSTRAINT C_FKEY_D FOREIGN KEY (C_D_ID, C_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
     );
     CREATE INDEX IDX_CUSTOMER ON CUSTOMER (C_W_ID,C_D_ID,C_LAST);
   */

  // Create schema first
  const bool own_schema = true;
  const bool adapt_table = false;
  const bool is_inlined = true;

  std::vector<catalog::Column> customer_columns;

  auto c_id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "C_ID", is_inlined);
  customer_columns.push_back(c_id_column);
  auto c_d_id_column = catalog::Column(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "C_D_ID", is_inlined);
  customer_columns.push_back(c_d_id_column);
  auto c_w_id_column = catalog::Column(VALUE_TYPE_SMALLINT, GetTypeSize(VALUE_TYPE_SMALLINT), "C_W_ID", is_inlined);
  customer_columns.push_back(c_w_id_column);
  auto c_first_name_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "C_FIRST", is_inlined);
  customer_columns.push_back(c_first_name_column);
  auto c_middle_name_column = catalog::Column(VALUE_TYPE_VARCHAR, 2, "C_MIDDLE", is_inlined);
  customer_columns.push_back(c_middle_name_column);
  auto c_last_name_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "C_LAST", is_inlined);
  customer_columns.push_back(c_last_name_column);
  auto c_street_1_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "C_STREET_1", is_inlined);
  customer_columns.push_back(c_street_1_column);
  auto c_street_2_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "C_STREET_2", is_inlined);
  customer_columns.push_back(c_street_2_column);
  auto c_city_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "C_CITY", is_inlined);
  customer_columns.push_back(c_city_column);
  auto c_state_column = catalog::Column(VALUE_TYPE_VARCHAR, 2, "C_STATE", is_inlined);
  customer_columns.push_back(c_state_column);
  auto c_zip_column = catalog::Column(VALUE_TYPE_VARCHAR, 9, "C_ZIP", is_inlined);
  customer_columns.push_back(c_zip_column);
  auto c_phone_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "C_PHONE", is_inlined);
  customer_columns.push_back(c_phone_column);
  auto c_since_column = catalog::Column(VALUE_TYPE_TIMESTAMP, GetTypeSize(VALUE_TYPE_TIMESTAMP), "C_SINCE", is_inlined);
  customer_columns.push_back(c_since_column);
  auto c_credit_column = catalog::Column(VALUE_TYPE_VARCHAR, 2, "C_CREDIT", is_inlined);
  customer_columns.push_back(c_credit_column);
  auto c_credit_lim_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "C_CREDIT_LIM", is_inlined);
  customer_columns.push_back(c_credit_lim_column);
  auto c_discount_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "C_DISCOUNT", is_inlined);
  customer_columns.push_back(c_discount_column);
  auto c_balance_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "C_BALANCE", is_inlined);
  customer_columns.push_back(c_balance_column);
  auto c_ytd_payment_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "C_YTD_PAYMENT", is_inlined);
  customer_columns.push_back(c_ytd_payment_column);
  auto c_payment_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "C_PAYMENT_CNT", is_inlined);
  customer_columns.push_back(c_payment_column);
  auto c_delivery_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "C_DELIVERY_CNT", is_inlined);
  customer_columns.push_back(c_delivery_column);
  auto c_data_column = catalog::Column(VALUE_TYPE_VARCHAR, 64, "C_DATA", is_inlined);
  customer_columns.push_back(c_data_column);

  catalog::Schema *table_schema = new catalog::Schema(customer_columns);
  std::string table_name("CUSTOMER");

  customer_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid,
      customer_table_oid,
      table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema,
      adapt_table);

  tpcc_database->AddTable(customer_table);

  // Primary index on C_W_ID, C_D_ID, C_ID
  std::vector<oid_t> key_attrs;

  auto tuple_schema = customer_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0, 1, 2};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "customer_pkey",
      customer_table_pkey_index_oid,
      INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
  customer_table->AddIndex(pkey_index);

  // Secondary index on C_W_ID, C_D_ID, C_LAST
  key_attrs.clear();

  key_attrs = {1, 2, 5};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "customer_skey",
      customer_table_skey_index_oid,
      INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, unique);

  index::Index *skey_index = index::IndexFactory::GetInstance(index_metadata);
  customer_table->AddIndex(skey_index);

}

void CreateHistoryTable() {
  /*
    CREATE TABLE HISTORY (
    H_C_ID INTEGER DEFAULT NULL,
    H_C_D_ID TINYINT DEFAULT NULL,
    H_C_W_ID SMALLINT DEFAULT NULL,
    H_D_ID TINYINT DEFAULT NULL,
    H_W_ID SMALLINT DEFAULT '0' NOT NULL,
    H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    H_AMOUNT FLOAT DEFAULT NULL,
    H_DATA VARCHAR(32) DEFAULT NULL,
    CONSTRAINT H_FKEY_C FOREIGN KEY (H_C_ID, H_C_D_ID, H_C_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID),
    CONSTRAINT H_FKEY_D FOREIGN KEY (H_D_ID, H_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
    );
   */

  // Create schema first
  const bool own_schema = true;
  const bool adapt_table = false;
  const bool is_inlined = true;

  std::vector<catalog::Column> history_columns;

  auto h_c_id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "H_C_ID", is_inlined);
  history_columns.push_back(h_c_id_column);
  auto h_c_d_id_column = catalog::Column(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "H_C_D_ID", is_inlined);
  history_columns.push_back(h_c_d_id_column);
  auto h_c_w_id_column = catalog::Column(VALUE_TYPE_SMALLINT, GetTypeSize(VALUE_TYPE_SMALLINT), "H_C_W_ID", is_inlined);
  history_columns.push_back(h_c_w_id_column);
  auto h_d_id_column = catalog::Column(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "H_D_ID", is_inlined);
  history_columns.push_back(h_d_id_column);
  auto h_w_id_column = catalog::Column(VALUE_TYPE_SMALLINT, GetTypeSize(VALUE_TYPE_SMALLINT), "H_W_ID", is_inlined);
  history_columns.push_back(h_w_id_column);
  auto h_date_column = catalog::Column(VALUE_TYPE_TIMESTAMP, GetTypeSize(VALUE_TYPE_TIMESTAMP), "H_DATE", is_inlined);
  history_columns.push_back(h_date_column);
  auto h_amount_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "H_AMOUNT", is_inlined);
  history_columns.push_back(h_amount_column);
  auto h_data_column = catalog::Column(VALUE_TYPE_VARCHAR, 64, "H_DATA", is_inlined);
  history_columns.push_back(h_data_column);

  catalog::Schema *table_schema = new catalog::Schema(history_columns);
  std::string table_name("CUSTOMER");

  history_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid,
      history_table_oid,
      table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema,
      adapt_table);

  tpcc_database->AddTable(history_table);

}

void CreateStockTable() {
  /*
   CREATE TABLE STOCK (
   S_I_ID INTEGER DEFAULT '0' NOT NULL REFERENCES ITEM (I_ID),
   S_W_ID SMALLINT DEFAULT '0 ' NOT NULL REFERENCES WAREHOUSE (W_ID),
   S_QUANTITY INTEGER DEFAULT '0' NOT NULL,
   S_DIST_01 VARCHAR(32) DEFAULT NULL,
   S_DIST_02 VARCHAR(32) DEFAULT NULL,
   S_DIST_03 VARCHAR(32) DEFAULT NULL,
   S_DIST_04 VARCHAR(32) DEFAULT NULL,
   S_DIST_05 VARCHAR(32) DEFAULT NULL,
   S_DIST_06 VARCHAR(32) DEFAULT NULL,
   S_DIST_07 VARCHAR(32) DEFAULT NULL,
   S_DIST_08 VARCHAR(32) DEFAULT NULL,
   S_DIST_09 VARCHAR(32) DEFAULT NULL,
   S_DIST_10 VARCHAR(32) DEFAULT NULL,
   S_YTD INTEGER DEFAULT NULL,
   S_ORDER_CNT INTEGER DEFAULT NULL,
   S_REMOTE_CNT INTEGER DEFAULT NULL,
   S_DATA VARCHAR(64) DEFAULT NULL,
   PRIMARY KEY (S_W_ID,S_I_ID)
   );
   */

  // Create schema first
  const bool own_schema = true;
  const bool adapt_table = false;
  const bool is_inlined = true;

  std::vector<catalog::Column> stock_columns;

  auto s_i_id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "S_I_ID", is_inlined);
  stock_columns.push_back(s_i_id_column);
  auto s_w_id_column = catalog::Column(VALUE_TYPE_SMALLINT, GetTypeSize(VALUE_TYPE_SMALLINT), "S_W_ID", is_inlined);
  stock_columns.push_back(s_w_id_column);
  auto s_quantity_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "S_QUANTITY", is_inlined);
  stock_columns.push_back(s_quantity_column);
  auto s_dist_01_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "S_DIST_01", is_inlined);
  stock_columns.push_back(s_dist_01_column);
  auto s_dist_02_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "S_DIST_02", is_inlined);
  stock_columns.push_back(s_dist_02_column);
  auto s_dist_03_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "S_DIST_03", is_inlined);
  stock_columns.push_back(s_dist_03_column);
  auto s_dist_04_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "S_DIST_04", is_inlined);
  stock_columns.push_back(s_dist_04_column);
  auto s_dist_05_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "S_DIST_05", is_inlined);
  stock_columns.push_back(s_dist_05_column);
  auto s_dist_06_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "S_DIST_06", is_inlined);
  stock_columns.push_back(s_dist_06_column);
  auto s_dist_07_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "S_DIST_07", is_inlined);
  stock_columns.push_back(s_dist_07_column);
  auto s_dist_08_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "S_DIST_08", is_inlined);
  stock_columns.push_back(s_dist_08_column);
  auto s_dist_09_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "S_DIST_09", is_inlined);
  stock_columns.push_back(s_dist_09_column);
  auto s_dist_10_column = catalog::Column(VALUE_TYPE_VARCHAR, 32, "S_DIST_10", is_inlined);
  stock_columns.push_back(s_dist_10_column);
  auto s_ytd_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "S_YTD", is_inlined);
  stock_columns.push_back(s_ytd_column);
  auto s_order_cnt_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "S_ORDER_CNT", is_inlined);
  stock_columns.push_back(s_order_cnt_column);
  auto s_discount_cnt_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "S_REMOTE_CNT", is_inlined);
  stock_columns.push_back(s_discount_cnt_column);
  auto s_data_column = catalog::Column(VALUE_TYPE_VARCHAR, 64, "S_DATA", is_inlined);
  stock_columns.push_back(s_data_column);

  catalog::Schema *table_schema = new catalog::Schema(stock_columns);
  std::string table_name("STOCK");

  stock_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid,
      stock_table_oid,
      table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema,
      adapt_table);

  tpcc_database->AddTable(stock_table);

  // Primary index on S_I_ID, S_W_ID
  std::vector<oid_t> key_attrs;

  auto tuple_schema = stock_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0, 1};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "stock_pkey",
      stock_table_pkey_index_oid,
      INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
  stock_table->AddIndex(pkey_index);

}

void CreateOrdersTable() {
  /*
   CREATE TABLE ORDERS (
   O_ID INTEGER DEFAULT '0' NOT NULL,
   O_C_ID INTEGER DEFAULT NULL,
   O_D_ID TINYINT DEFAULT '0' NOT NULL,
   O_W_ID SMALLINT DEFAULT '0' NOT NULL,
   O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   O_CARRIER_ID INTEGER DEFAULT NULL,
   O_OL_CNT INTEGER DEFAULT NULL,
   O_ALL_LOCAL INTEGER DEFAULT NULL,
   PRIMARY KEY (O_W_ID,O_D_ID,O_ID),
   UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID),
   CONSTRAINT O_FKEY_C FOREIGN KEY (O_C_ID, O_D_ID, O_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID)
   );
   CREATE INDEX IDX_ORDERS ON ORDERS (O_W_ID,O_D_ID,O_C_ID);
   */

  // Create schema first
  const bool own_schema = true;
  const bool adapt_table = false;
  const bool is_inlined = true;

  std::vector<catalog::Column> orders_columns;

  auto o_id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "O_ID", is_inlined);
  orders_columns.push_back(o_id_column);
  auto o_c_id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "O_C_ID", is_inlined);
  orders_columns.push_back(o_c_id_column);
  auto o_d_id_column = catalog::Column(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "O_D_ID", is_inlined);
  orders_columns.push_back(o_d_id_column);
  auto o_w_id_column = catalog::Column(VALUE_TYPE_SMALLINT, GetTypeSize(VALUE_TYPE_SMALLINT), "O_W_ID", is_inlined);
  orders_columns.push_back(o_w_id_column);
  auto o_entry_d_column = catalog::Column(VALUE_TYPE_TIMESTAMP, GetTypeSize(VALUE_TYPE_TIMESTAMP), "O_ENTRY_D", is_inlined);
  orders_columns.push_back(o_entry_d_column);
  auto o_carrier_id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "O_CARRIER_ID", is_inlined);
  orders_columns.push_back(o_carrier_id_column);
  auto o_ol_cnt_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "O_OL_CNT", is_inlined);
  orders_columns.push_back(o_ol_cnt_column);
  auto o_all_local_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "O_ALL_LOCAL", is_inlined);
  orders_columns.push_back(o_all_local_column);

  catalog::Schema *table_schema = new catalog::Schema(orders_columns);
  std::string table_name("ORDERS");

  orders_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid,
      orders_table_oid,
      table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema,
      adapt_table);

  tpcc_database->AddTable(orders_table);

  // Primary index on O_ID, O_D_ID, O_W_ID
  std::vector<oid_t> key_attrs;

  auto tuple_schema = orders_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0, 2, 3};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "orders_pkey",
      orders_table_pkey_index_oid,
      INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
  orders_table->AddIndex(pkey_index);

  // Secondary index on O_C_ID, O_D_ID, O_W_ID
  key_attrs.clear();

  key_attrs = {1, 2, 3};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "orders_skey",
      orders_table_skey_index_oid,
      INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, unique);

  index::Index *skey_index = index::IndexFactory::GetInstance(index_metadata);
  orders_table->AddIndex(skey_index);
}

void CreateNewOrderTable() {
  /*
   CREATE TABLE NEW_ORDER (
   NO_O_ID INTEGER DEFAULT '0' NOT NULL,
   NO_D_ID TINYINT DEFAULT '0' NOT NULL,
   NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
   CONSTRAINT NO_PK_TREE PRIMARY KEY (NO_D_ID,NO_W_ID,NO_O_ID),
   CONSTRAINT NO_FKEY_O FOREIGN KEY (NO_O_ID, NO_D_ID, NO_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID)
   );
   */

  // Create schema first
  const bool own_schema = true;
  const bool adapt_table = false;
  const bool is_inlined = true;

  std::vector<catalog::Column> new_order_columns;

  auto no_o_id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "NO_O_ID", is_inlined);
  new_order_columns.push_back(no_o_id_column);
  auto no_d_id_column = catalog::Column(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "NO_D_ID", is_inlined);
  new_order_columns.push_back(no_d_id_column);
  auto no_w_id_column = catalog::Column(VALUE_TYPE_SMALLINT, GetTypeSize(VALUE_TYPE_SMALLINT), "NO_W_ID", is_inlined);
  new_order_columns.push_back(no_w_id_column);

  catalog::Schema *table_schema = new catalog::Schema(new_order_columns);
  std::string table_name("NEW_ORDER");

  new_order_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid,
      new_order_table_oid,
      table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema,
      adapt_table);

  tpcc_database->AddTable(new_order_table);

  // Primary index on NO_O_ID, NO_D_ID, NO_W_ID
  std::vector<oid_t> key_attrs;

  auto tuple_schema = new_order_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0, 1, 2};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "new_order_pkey",
      new_order_table_pkey_index_oid,
      INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
  new_order_table->AddIndex(pkey_index);

}

void CreateOrderLineTable() {
  /*
   CREATE TABLE ORDER_LINE (
   OL_O_ID INTEGER DEFAULT '0' NOT NULL,
   OL_D_ID TINYINT DEFAULT '0' NOT NULL,
   OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
   OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
   OL_I_ID INTEGER DEFAULT NULL,
   OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
   OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
   OL_QUANTITY INTEGER DEFAULT NULL,
   OL_AMOUNT FLOAT DEFAULT NULL,
   OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
   PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER),
   CONSTRAINT OL_FKEY_O FOREIGN KEY (OL_O_ID, OL_D_ID, OL_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID),
   CONSTRAINT OL_FKEY_S FOREIGN KEY (OL_I_ID, OL_SUPPLY_W_ID) REFERENCES STOCK (S_I_ID, S_W_ID)
   );
   CREATE INDEX IDX_ORDER_LINE_TREE ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);
   */

  // Create schema first
  const bool own_schema = true;
  const bool adapt_table = false;
  const bool is_inlined = true;

  std::vector<catalog::Column> order_line_columns;

  auto ol_o_id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "OL_O_ID", is_inlined);
  order_line_columns.push_back(ol_o_id_column);
  auto ol_d_id_column = catalog::Column(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "OL_D_ID", is_inlined);
  order_line_columns.push_back(ol_d_id_column);
  auto ol_w_id_column = catalog::Column(VALUE_TYPE_SMALLINT, GetTypeSize(VALUE_TYPE_SMALLINT), "OL_W_ID", is_inlined);
  order_line_columns.push_back(ol_w_id_column);
  auto ol_number_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "OL_NUMBER", is_inlined);
  order_line_columns.push_back(ol_number_column);
  auto ol_i_id_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "OL_I_ID", is_inlined);
  order_line_columns.push_back(ol_i_id_column);
  auto ol_supply_w_id_column = catalog::Column(VALUE_TYPE_SMALLINT, GetTypeSize(VALUE_TYPE_SMALLINT), "OL_SUPPLY_W_ID", is_inlined);
  order_line_columns.push_back(ol_supply_w_id_column);
  auto ol_delivery_d_column = catalog::Column(VALUE_TYPE_TIMESTAMP, GetTypeSize(VALUE_TYPE_TIMESTAMP), "OL_DELIVERY_D", is_inlined);
  order_line_columns.push_back(ol_delivery_d_column);
  auto ol_quantity_column = catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "OL_QUANTITY", is_inlined);
  order_line_columns.push_back(ol_quantity_column);
  auto ol_amount_column = catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "OL_AMOUNT", is_inlined);
  order_line_columns.push_back(ol_amount_column);
  auto ol_dist_info_column = catalog::Column(VALUE_TYPE_VARCHAR, 64, "OL_DIST_INFO", is_inlined);
  order_line_columns.push_back(ol_dist_info_column);

  catalog::Schema *table_schema = new catalog::Schema(order_line_columns);
  std::string table_name("ORDER_LINE");

  order_line_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid,
      order_line_table_oid,
      table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP,
      own_schema,
      adapt_table);

  tpcc_database->AddTable(order_line_table);

  // Primary index on OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER
  std::vector<oid_t> key_attrs;

  auto tuple_schema = order_line_table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = {0, 1, 2, 3};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "order_line_pkey",
      order_line_table_pkey_index_oid,
      INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, unique);

  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);
  order_line_table->AddIndex(pkey_index);

  // Secondary index on OL_O_ID, OL_D_ID, OL_W_ID
  key_attrs.clear();

  key_attrs = {0, 1, 2};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  unique = true;

  index_metadata = new index::IndexMetadata(
      "order_line_skey",
      order_line_table_skey_index_oid,
      INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, unique);

  index::Index *skey_index = index::IndexFactory::GetInstance(index_metadata);
  order_line_table->AddIndex(skey_index);


}

void CreateTPCCDatabase() {
  /////////////////////////////////////////////////////////
  // Create tables
  /////////////////////////////////////////////////////////

  // Clean up
  delete tpcc_database;
  tpcc_database = nullptr;
  warehouse_table = nullptr;
  district_table = nullptr;
  item_table = nullptr;
  customer_table = nullptr;
  history_table = nullptr;
  stock_table = nullptr;
  new_order_table = nullptr;
  order_line_table = nullptr;

  auto& manager = catalog::Manager::GetInstance();
  tpcc_database = new storage::Database(tpcc_database_oid);
  manager.AddDatabase(tpcc_database);

}

void LoadTPCCDatabase() {
  /////////////////////////////////////////////////////////
  // Load in the data
  /////////////////////////////////////////////////////////

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  txn_manager.CommitTransaction(txn);
}


}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
