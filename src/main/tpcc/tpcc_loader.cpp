//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_loader.cpp
//
// Identification: src/main/tpcc/tpcc_loader.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cstring>
#include <random>

#include "benchmark/tpcc/tpcc_loader.h"
#include "benchmark/tpcc/tpcc_configuration.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/insert_executor.h"
#include "executor/executor_context.h"
#include "expression/constant_value_expression.h"
#include "expression/expression_util.h"
#include "index/index_factory.h"
#include "planner/insert_plan.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "storage/database.h"
#include "common/internal_types.h"


// Logging mode
// extern peloton::LoggingType peloton_logging_mode;

// disable unused const variable warning for clang
#ifdef __APPLE__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-const-variable"
#endif

namespace peloton {
namespace benchmark {
namespace tpcc {

/////////////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////////////

const size_t name_length = 32;
const size_t middle_name_length = 2;
const size_t data_length = 64;
const size_t state_length = 16;
const size_t zip_length = 9;
const size_t street_length = 32;
const size_t city_length = 32;
const size_t credit_length = 2;
const size_t phone_length = 32;
const size_t dist_length = 32;

double item_min_price = 1.0;
double item_max_price = 100.0;

double warehouse_name_length = 16;
double warehouse_min_tax = 0.0;
double warehouse_max_tax = 0.2;
double warehouse_initial_ytd = 300000.00f;

double district_name_length = 16;
double district_min_tax = 0.0;
double district_max_tax = 0.2;
double district_initial_ytd = 30000.00f;

std::string customers_good_credit = "GC";
std::string customers_bad_credit = "BC";
double customers_bad_credit_ratio = 0.1;
double customers_init_credit_lim = 50000.0;
double customers_min_discount = 0;
double customers_max_discount = 0.5;
double customers_init_balance = -10.0;
double customers_init_ytd = 10.0;
int customers_init_payment_cnt = 1;
int customers_init_delivery_cnt = 0;

double history_init_amount = 10.0;
size_t history_data_length = 32;

int orders_min_ol_cnt = 5;
int orders_max_ol_cnt = 15;
int orders_init_all_local = 1;
int orders_null_carrier_id = 0;
int orders_min_carrier_id = 1;
int orders_max_carrier_id = 10;

int new_orders_per_district = 900;  // 900

int order_line_init_quantity = 5;
int order_line_max_ol_quantity = 10;
double order_line_min_amount = 0.01;
size_t order_line_dist_info_length = 32;

double stock_original_ratio = 0.1;
int stock_min_quantity = 10;
int stock_max_quantity = 100;
int stock_dist_count = 10;

double payment_min_amount = 1.0;
double payment_max_amount = 5000.0;

int stock_min_threshold = 10;
int stock_max_threshold = 20;

double new_order_remote_txns = 0.01;

const int syllable_count = 10;
const char *syllables[syllable_count] = {"BAR",   "OUGHT", "ABLE", "PRI",
                                         "PRES",  "ESES",  "ANTI", "CALLY",
                                         "ATION", "EING"};

const std::string data_constant = std::string("FOO");

NURandConstant nu_rand_const;

/////////////////////////////////////////////////////////
// Create the tables
/////////////////////////////////////////////////////////

storage::Database *tpcc_database;
storage::DataTable *warehouse_table;
storage::DataTable *district_table;
storage::DataTable *item_table;
storage::DataTable *customer_table;
storage::DataTable *history_table;
storage::DataTable *stock_table;
storage::DataTable *orders_table;
storage::DataTable *new_order_table;
storage::DataTable *order_line_table;

const bool own_schema = true;
const bool adapt_table = false;
const bool is_inlined = false;
const bool unique_index = false;
const bool allocate = true;

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
  std::vector<catalog::Column> warehouse_columns;

  auto w_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "W_ID", is_inlined);
  warehouse_columns.push_back(w_id_column);
  auto w_name_column = catalog::Column(
      type::TypeId::VARCHAR, warehouse_name_length, "W_NAME", is_inlined);
  warehouse_columns.push_back(w_name_column);
  auto w_street_1_column = catalog::Column(type::TypeId::VARCHAR, street_length,
                                           "W_STREET_1", is_inlined);
  warehouse_columns.push_back(w_street_1_column);
  auto w_street_2_column = catalog::Column(type::TypeId::VARCHAR, street_length,
                                           "W_STREET_2", is_inlined);
  warehouse_columns.push_back(w_street_2_column);
  auto w_city_column =
      catalog::Column(type::TypeId::VARCHAR, city_length, "W_CITY", is_inlined);
  warehouse_columns.push_back(w_city_column);
  auto w_state_column =
      catalog::Column(type::TypeId::VARCHAR, state_length, "W_STATE", is_inlined);
  warehouse_columns.push_back(w_state_column);
  auto w_zip_column =
      catalog::Column(type::TypeId::VARCHAR, zip_length, "W_ZIP", is_inlined);
  warehouse_columns.push_back(w_zip_column);
  auto w_tax_column = catalog::Column(
      type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL), "W_TAX", is_inlined);
  warehouse_columns.push_back(w_tax_column);
  auto w_ytd_column = catalog::Column(
      type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL), "W_YTD", is_inlined);
  warehouse_columns.push_back(w_ytd_column);

  catalog::Schema *table_schema = new catalog::Schema(warehouse_columns);
  std::string table_name("WAREHOUSE");

  warehouse_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid, warehouse_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tpcc_database->AddTable(warehouse_table);

  // Primary index on W_ID
  std::vector<oid_t> key_attrs = {0};

  auto tuple_schema = warehouse_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
    "warehouse_pkey", warehouse_table_pkey_index_oid, warehouse_table_oid,
    tpcc_database_oid, state.index, IndexConstraintType::PRIMARY_KEY,
    tuple_schema, key_schema, key_attrs, unique);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));

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
  std::vector<catalog::Column> district_columns;

  auto d_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER), "D_ID", is_inlined);
  district_columns.push_back(d_id_column);
  auto d_w_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "D_W_ID", is_inlined);
  district_columns.push_back(d_w_id_column);
  auto d_name_column = catalog::Column(type::TypeId::VARCHAR, district_name_length,
                                       "D_NAME", is_inlined);
  district_columns.push_back(d_name_column);
  auto d_street_1_column = catalog::Column(type::TypeId::VARCHAR, street_length,
                                           "D_STREET_1", is_inlined);
  district_columns.push_back(d_street_1_column);
  auto d_street_2_column = catalog::Column(type::TypeId::VARCHAR, street_length,
                                           "D_STREET_2", is_inlined);
  district_columns.push_back(d_street_2_column);
  auto d_city_column =
      catalog::Column(type::TypeId::VARCHAR, city_length, "D_CITY", is_inlined);
  district_columns.push_back(d_city_column);
  auto d_state_column =
      catalog::Column(type::TypeId::VARCHAR, state_length, "D_STATE", is_inlined);
  district_columns.push_back(d_state_column);
  auto d_zip_column =
      catalog::Column(type::TypeId::VARCHAR, zip_length, "D_ZIP", is_inlined);
  district_columns.push_back(d_zip_column);
  auto d_tax_column = catalog::Column(
      type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL), "D_TAX", is_inlined);
  district_columns.push_back(d_tax_column);
  auto d_ytd_column = catalog::Column(
      type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL), "D_YTD", is_inlined);
  district_columns.push_back(d_ytd_column);
  auto d_next_o_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "D_NEXT_O_ID", is_inlined);
  district_columns.push_back(d_next_o_id_column);

  catalog::Schema *table_schema = new catalog::Schema(district_columns);
  std::string table_name("DISTRICT");

  district_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid, district_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tpcc_database->AddTable(district_table);

  // Primary index on D_ID, D_W_ID
  std::vector<oid_t> key_attrs = {0, 1};

  auto tuple_schema = district_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "district_pkey", district_table_pkey_index_oid,
      district_table_pkey_index_oid, district_table_oid, state.index,
      IndexConstraintType::PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
      unique);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));

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
  std::vector<catalog::Column> item_columns;

  auto i_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER), "I_ID", is_inlined);
  item_columns.push_back(i_id_column);
  auto i_im_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "I_IM_ID", is_inlined);
  item_columns.push_back(i_im_id_column);
  auto i_name_column =
      catalog::Column(type::TypeId::VARCHAR, name_length, "I_NAME", is_inlined);
  item_columns.push_back(i_name_column);
  auto i_price_column = catalog::Column(
      type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL), "I_PRICE", is_inlined);
  item_columns.push_back(i_price_column);
  auto i_data_column =
      catalog::Column(type::TypeId::VARCHAR, data_length, "I_DATA", is_inlined);
  item_columns.push_back(i_data_column);

  catalog::Schema *table_schema = new catalog::Schema(item_columns);
  std::string table_name("ITEM");

  item_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid, item_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tpcc_database->AddTable(item_table);

  // Primary index on I_ID
  std::vector<oid_t> key_attrs = {0};

  auto tuple_schema = item_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "item_pkey", item_table_pkey_index_oid, item_table_oid, tpcc_database_oid,
      state.index, IndexConstraintType::PRIMARY_KEY, tuple_schema,
      key_schema, key_attrs, unique);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));
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
     CONSTRAINT C_FKEY_D FOREIGN KEY (C_D_ID, C_W_ID) REFERENCES DISTRICT (D_ID,
     D_W_ID)
     );
     CREATE INDEX IDX_CUSTOMER ON CUSTOMER (C_W_ID,C_D_ID,C_LAST);
   */

  // Create schema first
  std::vector<catalog::Column> customer_columns;

  auto c_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER), "C_ID", is_inlined);
  customer_columns.push_back(c_id_column);
  auto c_d_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "C_D_ID", is_inlined);
  customer_columns.push_back(c_d_id_column);
  auto c_w_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "C_W_ID", is_inlined);
  customer_columns.push_back(c_w_id_column);
  auto c_first_name_column =
      catalog::Column(type::TypeId::VARCHAR, name_length, "C_FIRST", is_inlined);
  customer_columns.push_back(c_first_name_column);
  auto c_middle_name_column = catalog::Column(
      type::TypeId::VARCHAR, middle_name_length, "C_MIDDLE", is_inlined);
  customer_columns.push_back(c_middle_name_column);
  auto c_last_name_column =
      catalog::Column(type::TypeId::VARCHAR, name_length, "C_LAST", is_inlined);
  customer_columns.push_back(c_last_name_column);
  auto c_street_1_column = catalog::Column(type::TypeId::VARCHAR, street_length,
                                           "C_STREET_1", is_inlined);
  customer_columns.push_back(c_street_1_column);
  auto c_street_2_column = catalog::Column(type::TypeId::VARCHAR, street_length,
                                           "C_STREET_2", is_inlined);
  customer_columns.push_back(c_street_2_column);
  auto c_city_column =
      catalog::Column(type::TypeId::VARCHAR, city_length, "C_CITY", is_inlined);
  customer_columns.push_back(c_city_column);
  auto c_state_column =
      catalog::Column(type::TypeId::VARCHAR, state_length, "C_STATE", is_inlined);
  customer_columns.push_back(c_state_column);
  auto c_zip_column =
      catalog::Column(type::TypeId::VARCHAR, zip_length, "C_ZIP", is_inlined);
  customer_columns.push_back(c_zip_column);
  auto c_phone_column =
      catalog::Column(type::TypeId::VARCHAR, phone_length, "C_PHONE", is_inlined);
  customer_columns.push_back(c_phone_column);
  auto c_since_column =
      catalog::Column(type::TypeId::TIMESTAMP, type::Type::GetTypeSize(type::TypeId::TIMESTAMP),
                      "C_SINCE", is_inlined);
  customer_columns.push_back(c_since_column);
  auto c_credit_column = catalog::Column(type::TypeId::VARCHAR, credit_length,
                                         "C_CREDIT", is_inlined);
  customer_columns.push_back(c_credit_column);
  auto c_credit_lim_column =
      catalog::Column(type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL),
                      "C_CREDIT_LIM", is_inlined);
  customer_columns.push_back(c_credit_lim_column);
  auto c_discount_column =
      catalog::Column(type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL),
                      "C_DISCOUNT", is_inlined);
  customer_columns.push_back(c_discount_column);
  auto c_balance_column =
      catalog::Column(type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL),
                      "C_BALANCE", is_inlined);
  customer_columns.push_back(c_balance_column);
  auto c_ytd_payment_column =
      catalog::Column(type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL),
                      "C_YTD_PAYMENT", is_inlined);
  customer_columns.push_back(c_ytd_payment_column);
  auto c_payment_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "C_PAYMENT_CNT", is_inlined);
  customer_columns.push_back(c_payment_column);
  auto c_delivery_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "C_DELIVERY_CNT", is_inlined);
  customer_columns.push_back(c_delivery_column);
  auto c_data_column =
      catalog::Column(type::TypeId::VARCHAR, data_length, "C_DATA", is_inlined);
  customer_columns.push_back(c_data_column);

  catalog::Schema *table_schema = new catalog::Schema(customer_columns);
  std::string table_name("CUSTOMER");

  customer_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid, customer_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tpcc_database->AddTable(customer_table);

  auto tuple_schema = customer_table->GetSchema();
  std::vector<oid_t> key_attrs;
  catalog::Schema *key_schema = nullptr;
  index::IndexMetadata *index_metadata = nullptr;

  // Primary index on C_ID, C_D_ID, C_W_ID
  key_attrs = {0, 1, 2};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  index_metadata = new index::IndexMetadata(
    "customer_pkey", customer_table_pkey_index_oid, customer_table_oid,
    tpcc_database_oid, state.index, IndexConstraintType::PRIMARY_KEY,
    tuple_schema, key_schema, key_attrs, true);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));
  customer_table->AddIndex(pkey_index);

  // Secondary index on C_W_ID, C_D_ID, C_LAST
  key_attrs = {1, 2, 5};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  index_metadata = new index::IndexMetadata(
      "customer_skey", customer_table_skey_index_oid, customer_table_oid,
      tpcc_database_oid, state.index, IndexConstraintType::INVALID,
      tuple_schema, key_schema, key_attrs, false);

  std::shared_ptr<index::Index> skey_index(
      index::IndexFactory::GetIndex(index_metadata));
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
    CONSTRAINT H_FKEY_C FOREIGN KEY (H_C_ID, H_C_D_ID, H_C_W_ID) REFERENCES
    CUSTOMER (C_ID, C_D_ID, C_W_ID),
    CONSTRAINT H_FKEY_D FOREIGN KEY (H_D_ID, H_W_ID) REFERENCES DISTRICT (D_ID,
    D_W_ID)
    );
   */

  // Create schema first
  std::vector<catalog::Column> history_columns;

  auto h_c_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "H_C_ID", is_inlined);
  history_columns.push_back(h_c_id_column);
  auto h_c_d_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "H_C_D_ID", is_inlined);
  history_columns.push_back(h_c_d_id_column);
  auto h_c_w_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "H_C_W_ID", is_inlined);
  history_columns.push_back(h_c_w_id_column);
  auto h_d_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "H_D_ID", is_inlined);
  history_columns.push_back(h_d_id_column);
  auto h_w_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "H_W_ID", is_inlined);
  history_columns.push_back(h_w_id_column);
  auto h_date_column =
      catalog::Column(type::TypeId::TIMESTAMP, type::Type::GetTypeSize(type::TypeId::TIMESTAMP),
                      "H_DATE", is_inlined);
  history_columns.push_back(h_date_column);
  auto h_amount_column =
      catalog::Column(type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL),
                      "H_AMOUNT", is_inlined);
  history_columns.push_back(h_amount_column);
  auto h_data_column = catalog::Column(type::TypeId::VARCHAR, history_data_length,
                                       "H_DATA", is_inlined);
  history_columns.push_back(h_data_column);

  catalog::Schema *table_schema = new catalog::Schema(history_columns);
  std::string table_name("HISTORY");

  history_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid, history_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

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
  std::vector<catalog::Column> stock_columns;

  auto s_i_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "S_I_ID", is_inlined);
  stock_columns.push_back(s_i_id_column);
  auto s_w_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "S_W_ID", is_inlined);
  stock_columns.push_back(s_w_id_column);
  auto s_quantity_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "S_QUANTITY", is_inlined);
  stock_columns.push_back(s_quantity_column);
  auto s_dist_01_column =
      catalog::Column(type::TypeId::VARCHAR, dist_length, "S_DIST_01", is_inlined);
  stock_columns.push_back(s_dist_01_column);
  auto s_dist_02_column =
      catalog::Column(type::TypeId::VARCHAR, dist_length, "S_DIST_02", is_inlined);
  stock_columns.push_back(s_dist_02_column);
  auto s_dist_03_column =
      catalog::Column(type::TypeId::VARCHAR, dist_length, "S_DIST_03", is_inlined);
  stock_columns.push_back(s_dist_03_column);
  auto s_dist_04_column =
      catalog::Column(type::TypeId::VARCHAR, dist_length, "S_DIST_04", is_inlined);
  stock_columns.push_back(s_dist_04_column);
  auto s_dist_05_column =
      catalog::Column(type::TypeId::VARCHAR, dist_length, "S_DIST_05", is_inlined);
  stock_columns.push_back(s_dist_05_column);
  auto s_dist_06_column =
      catalog::Column(type::TypeId::VARCHAR, dist_length, "S_DIST_06", is_inlined);
  stock_columns.push_back(s_dist_06_column);
  auto s_dist_07_column =
      catalog::Column(type::TypeId::VARCHAR, dist_length, "S_DIST_07", is_inlined);
  stock_columns.push_back(s_dist_07_column);
  auto s_dist_08_column =
      catalog::Column(type::TypeId::VARCHAR, dist_length, "S_DIST_08", is_inlined);
  stock_columns.push_back(s_dist_08_column);
  auto s_dist_09_column =
      catalog::Column(type::TypeId::VARCHAR, dist_length, "S_DIST_09", is_inlined);
  stock_columns.push_back(s_dist_09_column);
  auto s_dist_10_column =
      catalog::Column(type::TypeId::VARCHAR, dist_length, "S_DIST_10", is_inlined);
  stock_columns.push_back(s_dist_10_column);
  auto s_ytd_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER), "S_YTD", is_inlined);
  stock_columns.push_back(s_ytd_column);
  auto s_order_cnt_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "S_ORDER_CNT", is_inlined);
  stock_columns.push_back(s_order_cnt_column);
  auto s_discount_cnt_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "S_REMOTE_CNT", is_inlined);
  stock_columns.push_back(s_discount_cnt_column);
  auto s_data_column =
      catalog::Column(type::TypeId::VARCHAR, data_length, "S_DATA", is_inlined);
  stock_columns.push_back(s_data_column);

  catalog::Schema *table_schema = new catalog::Schema(stock_columns);
  std::string table_name("STOCK");

  stock_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid, stock_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tpcc_database->AddTable(stock_table);

  // Primary index on S_I_ID, S_W_ID
  std::vector<oid_t> key_attrs = {0, 1};

  auto tuple_schema = stock_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "stock_pkey", stock_table_pkey_index_oid, stock_table_oid,
      tpcc_database_oid, state.index, IndexConstraintType::PRIMARY_KEY,
      tuple_schema, key_schema, key_attrs, unique);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));
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
   CONSTRAINT O_FKEY_C FOREIGN KEY (O_C_ID, O_D_ID, O_W_ID) REFERENCES CUSTOMER
   (C_ID, C_D_ID, C_W_ID)
   );
   CREATE INDEX IDX_ORDERS ON ORDERS (O_W_ID,O_D_ID,O_C_ID);
   */

  // Create schema first
  std::vector<catalog::Column> orders_columns;

  auto o_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER), "O_ID", is_inlined);
  orders_columns.push_back(o_id_column);
  auto o_c_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "O_C_ID", is_inlined);
  orders_columns.push_back(o_c_id_column);
  auto o_d_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "O_D_ID", is_inlined);
  orders_columns.push_back(o_d_id_column);
  auto o_w_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "O_W_ID", is_inlined);
  orders_columns.push_back(o_w_id_column);
  auto o_entry_d_column =
      catalog::Column(type::TypeId::TIMESTAMP, type::Type::GetTypeSize(type::TypeId::TIMESTAMP),
                      "O_ENTRY_D", is_inlined);
  orders_columns.push_back(o_entry_d_column);
  auto o_carrier_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "O_CARRIER_ID", is_inlined);
  orders_columns.push_back(o_carrier_id_column);
  auto o_ol_cnt_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "O_OL_CNT", is_inlined);
  orders_columns.push_back(o_ol_cnt_column);
  auto o_all_local_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "O_ALL_LOCAL", is_inlined);
  orders_columns.push_back(o_all_local_column);

  catalog::Schema *table_schema = new catalog::Schema(orders_columns);
  std::string table_name("ORDERS");

  orders_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid, orders_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tpcc_database->AddTable(orders_table);

  auto tuple_schema = customer_table->GetSchema();
  std::vector<oid_t> key_attrs;
  catalog::Schema *key_schema = nullptr;
  index::IndexMetadata *index_metadata = nullptr;

  // Primary index on O_ID, O_D_ID, O_W_ID
  key_attrs = {0, 2, 3};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  index_metadata = new index::IndexMetadata(
      "orders_pkey", orders_table_pkey_index_oid, orders_table_oid,
      tpcc_database_oid, state.index, IndexConstraintType::PRIMARY_KEY,
      tuple_schema, key_schema, key_attrs, true);


  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));
  orders_table->AddIndex(pkey_index);

  // Secondary index on O_C_ID, O_D_ID, O_W_ID
  key_attrs = {1, 2, 3};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  index_metadata = new index::IndexMetadata(
      "orders_skey", orders_table_skey_index_oid, orders_table_oid,
      tpcc_database_oid, state.index, IndexConstraintType::INVALID,
      tuple_schema, key_schema, key_attrs, false);

  std::shared_ptr<index::Index> skey_index(
      index::IndexFactory::GetIndex(index_metadata));
  orders_table->AddIndex(skey_index);
}

void CreateNewOrderTable() {
  /*
   CREATE TABLE NEW_ORDER (
   NO_O_ID INTEGER DEFAULT '0' NOT NULL,
   NO_D_ID TINYINT DEFAULT '0' NOT NULL,
   NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
   CONSTRAINT NO_PK_TREE PRIMARY KEY (NO_D_ID,NO_W_ID,NO_O_ID),
   CONSTRAINT NO_FKEY_O FOREIGN KEY (NO_O_ID, NO_D_ID, NO_W_ID) REFERENCES
   ORDERS (O_ID, O_D_ID, O_W_ID)
   );
   */

  // Create schema first
  std::vector<catalog::Column> new_order_columns;

  auto no_o_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "NO_O_ID", is_inlined);
  new_order_columns.push_back(no_o_id_column);
  auto no_d_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "NO_D_ID", is_inlined);
  new_order_columns.push_back(no_d_id_column);
  auto no_w_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "NO_W_ID", is_inlined);
  new_order_columns.push_back(no_w_id_column);

  catalog::Schema *table_schema = new catalog::Schema(new_order_columns);
  std::string table_name("NEW_ORDER");

  new_order_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid, new_order_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tpcc_database->AddTable(new_order_table);

  // Primary index on NO_O_ID, NO_D_ID, NO_W_ID
  std::vector<oid_t> key_attrs = {0, 1, 2};

  auto tuple_schema = new_order_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
    "new_order_pkey", new_order_table_pkey_index_oid, new_order_table_oid,
    tpcc_database_oid, state.index, IndexConstraintType::PRIMARY_KEY,
    tuple_schema, key_schema, key_attrs, unique);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));
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
   CONSTRAINT OL_FKEY_O FOREIGN KEY (OL_O_ID, OL_D_ID, OL_W_ID) REFERENCES
   ORDERS (O_ID, O_D_ID, O_W_ID),
   CONSTRAINT OL_FKEY_S FOREIGN KEY (OL_I_ID, OL_SUPPLY_W_ID) REFERENCES STOCK
   (S_I_ID, S_W_ID)
   );
   CREATE INDEX IDX_ORDER_LINE_TREE ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);
   */

  // Create schema first
  std::vector<catalog::Column> order_line_columns;

  auto ol_o_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "OL_O_ID", is_inlined);
  order_line_columns.push_back(ol_o_id_column);
  auto ol_d_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "OL_D_ID", is_inlined);
  order_line_columns.push_back(ol_d_id_column);
  auto ol_w_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "OL_W_ID", is_inlined);
  order_line_columns.push_back(ol_w_id_column);
  auto ol_number_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "OL_NUMBER", is_inlined);
  order_line_columns.push_back(ol_number_column);
  auto ol_i_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "OL_I_ID", is_inlined);
  order_line_columns.push_back(ol_i_id_column);
  auto ol_supply_w_id_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "OL_SUPPLY_W_ID", is_inlined);
  order_line_columns.push_back(ol_supply_w_id_column);
  auto ol_delivery_d_column =
      catalog::Column(type::TypeId::TIMESTAMP, type::Type::GetTypeSize(type::TypeId::TIMESTAMP),
                      "OL_DELIVERY_D", is_inlined);
  order_line_columns.push_back(ol_delivery_d_column);
  auto ol_quantity_column =
      catalog::Column(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                      "OL_QUANTITY", is_inlined);
  order_line_columns.push_back(ol_quantity_column);
  auto ol_amount_column =
      catalog::Column(type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL),
                      "OL_AMOUNT", is_inlined);
  order_line_columns.push_back(ol_amount_column);
  auto ol_dist_info_column =
      catalog::Column(type::TypeId::VARCHAR, order_line_dist_info_length,
                      "OL_DIST_INFO", is_inlined);
  order_line_columns.push_back(ol_dist_info_column);

  catalog::Schema *table_schema = new catalog::Schema(order_line_columns);
  std::string table_name("ORDER_LINE");

  order_line_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid, order_line_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  tpcc_database->AddTable(order_line_table);

  auto tuple_schema = order_line_table->GetSchema();
  std::vector<oid_t> key_attrs;
  catalog::Schema *key_schema = nullptr;
  index::IndexMetadata *index_metadata = nullptr;

  // Primary index on OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER
  key_attrs = {0, 1, 2, 3};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  index_metadata = new index::IndexMetadata(
      "order_line_pkey", order_line_table_pkey_index_oid, order_line_table_oid,
      tpcc_database_oid, state.index, IndexConstraintType::PRIMARY_KEY,
      tuple_schema, key_schema, key_attrs, true);


  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));
  order_line_table->AddIndex(pkey_index);

  // Secondary index on OL_O_ID, OL_D_ID, OL_W_ID
  key_attrs = {0, 1, 2};
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  index_metadata = new index::IndexMetadata(
      "order_line_skey", order_line_table_skey_index_oid, order_line_table_oid,
      tpcc_database_oid, state.index, IndexConstraintType::INVALID,
      tuple_schema, key_schema, key_attrs, false);

  std::shared_ptr<index::Index> skey_index(
      index::IndexFactory::GetIndex(index_metadata));
  order_line_table->AddIndex(skey_index);
}

void CreateTPCCDatabase() {
  // Clean up
  delete tpcc_database;
  tpcc_database = nullptr;
  warehouse_table = nullptr;
  district_table = nullptr;
  item_table = nullptr;
  customer_table = nullptr;
  history_table = nullptr;
  stock_table = nullptr;
  orders_table = nullptr;
  new_order_table = nullptr;
  order_line_table = nullptr;

  auto catalog = catalog::Catalog::GetInstance();
  tpcc_database = new storage::Database(tpcc_database_oid);
  catalog->AddDatabase(tpcc_database);

  CreateWarehouseTable();
  CreateDistrictTable();
  CreateItemTable();
  CreateCustomerTable();
  CreateHistoryTable();
  CreateStockTable();
  CreateOrdersTable();
  CreateNewOrderTable();
  CreateOrderLineTable();
}

/////////////////////////////////////////////////////////
// Load in the tables
/////////////////////////////////////////////////////////

std::random_device rd;
std::mt19937 rng(rd());

// Create random NURand constants, appropriate for loading the database.
NURandConstant::NURandConstant() {
  c_last = GetRandomInteger(0, 255);
  c_id = GetRandomInteger(0, 1023);
  order_line_itme_id = GetRandomInteger(0, 8191);
}

// A non-uniform random number, as defined by TPC-C 2.1.6. (page 20).
int GetNURand(int a, int x, int y) {
  PL_ASSERT(x <= y);
  int c = nu_rand_const.c_last;

  if (a == 255) {
    c = nu_rand_const.c_last;
  } else if (a == 1023) {
    c = nu_rand_const.c_id;
  } else if (a == 8191) {
    c = nu_rand_const.order_line_itme_id;
  } else {
    PL_ASSERT(false);
  }

  return (((GetRandomInteger(0, a) | GetRandomInteger(x, y)) + c) %
          (y - x + 1)) +
         x;
}

// A last name as defined by TPC-C 4.3.2.3. Not actually random.
std::string GetLastName(int number) {
  PL_ASSERT(number >= 0 && number <= 999);

  int idx1 = number / 100;
  int idx2 = (number / 10 % 10);
  int idx3 = number % 10;

  char lastname_cstr[name_length];
  std::strcpy(lastname_cstr, syllables[idx1]);
  std::strcat(lastname_cstr, syllables[idx2]);
  std::strcat(lastname_cstr, syllables[idx3]);

  return std::string(lastname_cstr, name_length);
}

// A non-uniform random last name, as defined by TPC-C 4.3.2.3.
// The name will be limited to maxCID
std::string GetRandomLastName(int max_cid) {
  int min_cid = 999;
  if (max_cid - 1 < min_cid) {
    min_cid = max_cid - 1;
  }

  return GetLastName(GetNURand(255, 0, min_cid));
}

std::string GetRandomAlphaNumericString(const size_t string_length) {
  const char alphanumeric[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  std::uniform_int_distribution<> dist(0, sizeof(alphanumeric) - 1);

  char repeated_char = alphanumeric[dist(rng)];
  std::string sample(string_length, repeated_char);
  return sample;
}

bool GetRandomBoolean(double ratio) {
  double sample = (double)rand() / RAND_MAX;
  return (sample < ratio) ? true : false;
}

int GetRandomInteger(const int lower_bound, const int upper_bound) {
  std::uniform_int_distribution<> dist(lower_bound, upper_bound);

  int sample = dist(rng);
  return sample;
}

int GetRandomIntegerExcluding(const int lower_bound, const int upper_bound,
                              const int exclude_sample) {
  int sample;
  if (lower_bound == upper_bound) return lower_bound;

  while (1) {
    sample = GetRandomInteger(lower_bound, upper_bound);
    if (sample != exclude_sample) break;
  }
  return sample;
}

double GetRandomDouble(const double lower_bound, const double upper_bound) {
  std::uniform_real_distribution<> dist(lower_bound, upper_bound);

  double sample = dist(rng);
  return sample;
}

double GetRandomFixedPoint(int decimal_places, double minimum, double maximum) {
  PL_ASSERT(decimal_places > 0);
  PL_ASSERT(minimum < maximum);

  int multiplier = 1;
  for (int i = 0; i < decimal_places; ++i) {
    multiplier *= 10;
  }

  int int_min = (int)(minimum * multiplier + 0.5);
  int int_max = (int)(maximum * multiplier + 0.5);

  return GetRandomDouble(int_min, int_max) / (double)(multiplier);
}

std::string GetStreetName() {
  std::vector<std::string> street_names = {
      "5835 Alderson St",  "117  Ettwein St",    "1400 Fairstead Ln",
      "1501 Denniston St", "898  Flemington St", "2325 Eldridge St",
      "924  Lilac St",     "4299 Minnesota St",  "5498 Northumberland St",
      "5534 Phillips Ave"};

  std::uniform_int_distribution<> dist(0, street_names.size() - 1);
  return street_names[dist(rng)];
}

std::string GetZipCode() {
  std::vector<std::string> zip_codes = {"15215", "14155", "80284", "61845",
                                        "23146", "21456", "12345", "21561",
                                        "87752", "91095"};

  std::uniform_int_distribution<> dist(0, zip_codes.size() - 1);
  return zip_codes[dist(rng)];
}

std::string GetCityName() {
  std::vector<std::string> city_names = {
      "Madison",  "Pittsburgh", "New York",    "Seattle", "San Francisco",
      "Berkeley", "Palo Alto",  "Los Angeles", "Boston",  "Redwood Shores"};

  std::uniform_int_distribution<> dist(0, city_names.size() - 1);
  return city_names[dist(rng)];
}

std::string GetStateName() {
  std::vector<std::string> state_names = {"WI", "PA", "NY", "WA", "CA", "MA"};

  std::uniform_int_distribution<> dist(0, state_names.size() - 1);
  return state_names[dist(rng)];
}

int GetTimeStamp() {
  auto time_stamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  return time_stamp;
}

std::unique_ptr<storage::Tuple> BuildItemTuple(
    const int item_id, const std::unique_ptr<type::AbstractPool> &pool) {
  auto item_table_schema = item_table->GetSchema();
  std::unique_ptr<storage::Tuple> item_tuple(
      new storage::Tuple(item_table_schema, allocate));

  // I_ID
  item_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(item_id), nullptr);
  // I_IM_ID
  item_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(item_id * 10), nullptr);
  // I_NAME
  auto i_name = GetRandomAlphaNumericString(name_length);
  item_tuple->SetValue(2, type::ValueFactory::GetVarcharValue(i_name), pool.get());
  // I_PRICE
  double i_price = GetRandomDouble(item_min_price, item_max_price);
  item_tuple->SetValue(3, type::ValueFactory::GetDecimalValue(i_price), nullptr);
  // I_DATA
  auto i_data = GetRandomAlphaNumericString(data_length);
  item_tuple->SetValue(4, type::ValueFactory::GetVarcharValue(i_data), pool.get());

  return item_tuple;
}

std::unique_ptr<storage::Tuple> BuildWarehouseTuple(
    const int warehouse_id, const std::unique_ptr<type::AbstractPool> &pool) {
  auto warehouse_table_schema = warehouse_table->GetSchema();
  std::unique_ptr<storage::Tuple> warehouse_tuple(
      new storage::Tuple(warehouse_table_schema, allocate));

  // W_ID
  warehouse_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(warehouse_id),
                            nullptr);
  // W_NAME
  auto w_name = GetRandomAlphaNumericString(warehouse_name_length);
  warehouse_tuple->SetValue(1, type::ValueFactory::GetVarcharValue(w_name),
                            pool.get());
  // W_STREET_1, W_STREET_2
  auto w_street = GetStreetName();
  warehouse_tuple->SetValue(2, type::ValueFactory::GetVarcharValue(w_street),
                            pool.get());
  warehouse_tuple->SetValue(3, type::ValueFactory::GetVarcharValue(w_street),
                            pool.get());
  // W_CITY
  auto w_city = GetCityName();
  warehouse_tuple->SetValue(4, type::ValueFactory::GetVarcharValue(w_city),
                            pool.get());
  // W_STATE
  auto w_state = GetStateName();
  warehouse_tuple->SetValue(5, type::ValueFactory::GetVarcharValue(w_state),
                            pool.get());
  // W_ZIP
  auto w_zip = GetZipCode();
  warehouse_tuple->SetValue(6, type::ValueFactory::GetVarcharValue(w_zip), pool.get());
  // W_TAX
  double w_tax = GetRandomDouble(warehouse_min_tax, warehouse_max_tax);
  warehouse_tuple->SetValue(7, type::ValueFactory::GetDecimalValue(w_tax), nullptr);
  // W_YTD
  warehouse_tuple->SetValue(
      8, type::ValueFactory::GetDecimalValue(warehouse_initial_ytd), nullptr);

  return warehouse_tuple;
}

std::unique_ptr<storage::Tuple> BuildDistrictTuple(
    const int district_id, const int warehouse_id,
    const std::unique_ptr<type::AbstractPool> &pool) {
  auto district_table_schema = district_table->GetSchema();
  std::unique_ptr<storage::Tuple> district_tuple(
      new storage::Tuple(district_table_schema, allocate));

  // D_ID
  district_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(district_id),
                           nullptr);
  // D_W_ID
  district_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(warehouse_id),
                           nullptr);
  // D_NAME
  auto d_name = GetRandomAlphaNumericString(district_name_length);
  district_tuple->SetValue(2, type::ValueFactory::GetVarcharValue(d_name), pool.get());
  // D_STREET_1, D_STREET_2
  auto d_street = GetStreetName();
  district_tuple->SetValue(3, type::ValueFactory::GetVarcharValue(d_street),
                           pool.get());
  district_tuple->SetValue(4, type::ValueFactory::GetVarcharValue(d_street),
                           pool.get());
  // D_CITY
  auto d_city = GetCityName();
  district_tuple->SetValue(5, type::ValueFactory::GetVarcharValue(d_city), pool.get());
  // D_STATE
  auto d_state = GetStateName();
  district_tuple->SetValue(6, type::ValueFactory::GetVarcharValue(d_state),
                           pool.get());
  // D_ZIP
  auto d_zip = GetZipCode();
  district_tuple->SetValue(7, type::ValueFactory::GetVarcharValue(d_zip), pool.get());
  // D_TAX
  double d_tax = GetRandomDouble(district_min_tax, district_max_tax);
  district_tuple->SetValue(8, type::ValueFactory::GetDecimalValue(d_tax), nullptr);
  // D_YTD
  district_tuple->SetValue(
      9, type::ValueFactory::GetDecimalValue(district_initial_ytd), nullptr);
  // D_NEXT_O_ID
  auto next_o_id = state.customers_per_district + 1;
  district_tuple->SetValue(10, type::ValueFactory::GetIntegerValue(next_o_id),
                           nullptr);

  return district_tuple;
}

std::unique_ptr<storage::Tuple> BuildCustomerTuple(
    const int customer_id, const int district_id, const int warehouse_id,
    const std::unique_ptr<type::AbstractPool> &pool) {
  // Customer id begins from 0
  PL_ASSERT(customer_id >= 0 && customer_id < state.customers_per_district);

  auto customer_table_schema = customer_table->GetSchema();
  std::unique_ptr<storage::Tuple> customer_tuple(
      new storage::Tuple(customer_table_schema, allocate));

  // C_ID
  customer_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(customer_id),
                           nullptr);
  // C_D_ID
  customer_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(district_id),
                           nullptr);
  // C_W_ID
  customer_tuple->SetValue(2, type::ValueFactory::GetIntegerValue(warehouse_id),
                           nullptr);
  // C_FIRST, C_MIDDLE, C_LAST
  auto c_first = GetRandomAlphaNumericString(name_length);

  std::string c_last;

  // Here our customer id begins from 0
  if (customer_id <= 999) {
    c_last = GetLastName(customer_id);
  } else {
    c_last = GetRandomLastName(state.customers_per_district);
  }

  auto c_middle = GetRandomAlphaNumericString(middle_name_length);
  customer_tuple->SetValue(3, type::ValueFactory::GetVarcharValue(c_first),
                           pool.get());
  customer_tuple->SetValue(4, type::ValueFactory::GetVarcharValue(c_middle),
                           pool.get());
  customer_tuple->SetValue(5, type::ValueFactory::GetVarcharValue(c_last), pool.get());
  // C_STREET_1, C_STREET_2
  auto c_street = GetStreetName();
  customer_tuple->SetValue(6, type::ValueFactory::GetVarcharValue(c_street),
                           pool.get());
  customer_tuple->SetValue(7, type::ValueFactory::GetVarcharValue(c_street),
                           pool.get());
  // C_CITY
  auto c_city = GetCityName();
  customer_tuple->SetValue(8, type::ValueFactory::GetVarcharValue(c_city), pool.get());
  // C_STATE
  auto c_state = GetStateName();
  customer_tuple->SetValue(9, type::ValueFactory::GetVarcharValue(c_state),
                           pool.get());
  // C_ZIP
  auto c_zip = GetZipCode();
  customer_tuple->SetValue(10, type::ValueFactory::GetVarcharValue(c_zip), pool.get());
  // C_PHONE
  auto c_phone = GetRandomAlphaNumericString(phone_length);
  customer_tuple->SetValue(11, type::ValueFactory::GetVarcharValue(c_phone),
                           pool.get());
  // C_SINCE_TIMESTAMP
  auto c_since_timestamp = GetTimeStamp();
  customer_tuple->SetValue(
      12, type::ValueFactory::GetTimestampValue(c_since_timestamp), nullptr);
  // C_CREDIT
  auto c_bad_credit = GetRandomBoolean(customers_bad_credit_ratio);
  auto c_credit = c_bad_credit ? customers_bad_credit : customers_good_credit;
  customer_tuple->SetValue(13, type::ValueFactory::GetVarcharValue(c_credit),
                           pool.get());
  // C_CREDIT_LIM
  customer_tuple->SetValue(
      14, type::ValueFactory::GetDecimalValue(customers_init_credit_lim), nullptr);
  // C_DISCOUNT
  double c_discount =
      GetRandomDouble(customers_min_discount, customers_max_discount);
  customer_tuple->SetValue(15, type::ValueFactory::GetDecimalValue(c_discount),
                           nullptr);
  // C_BALANCE
  customer_tuple->SetValue(
      16, type::ValueFactory::GetDecimalValue(customers_init_balance), nullptr);
  // C_YTD_PAYMENT
  customer_tuple->SetValue(17, type::ValueFactory::GetDecimalValue(customers_init_ytd),
                           nullptr);
  // C_PAYMENT_CNT
  customer_tuple->SetValue(
      18, type::ValueFactory::GetDecimalValue(customers_init_payment_cnt), nullptr);
  // C_DELIVERY_CNT
  customer_tuple->SetValue(
      19, type::ValueFactory::GetDecimalValue(customers_init_delivery_cnt), nullptr);
  // C_DATA
  auto c_data = GetRandomAlphaNumericString(data_length);
  customer_tuple->SetValue(20, type::ValueFactory::GetVarcharValue(c_data),
                           pool.get());

  return customer_tuple;
}

std::unique_ptr<storage::Tuple> BuildHistoryTuple(
    const int customer_id, const int district_id, const int warehouse_id,
    const int history_district_id, const int history_warehouse_id,
    const std::unique_ptr<type::AbstractPool> &pool) {
  auto history_table_schema = history_table->GetSchema();
  std::unique_ptr<storage::Tuple> history_tuple(
      new storage::Tuple(history_table_schema, allocate));

  // H_C_ID
  history_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(customer_id),
                          nullptr);
  // H_C_D_ID
  history_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(district_id),
                          nullptr);
  // H_C_W_ID
  history_tuple->SetValue(2, type::ValueFactory::GetIntegerValue(warehouse_id),
                          nullptr);
  // H_D_ID
  history_tuple->SetValue(3, type::ValueFactory::GetIntegerValue(history_district_id),
                          nullptr);
  // H_W_ID
  history_tuple->SetValue(
      4, type::ValueFactory::GetIntegerValue(history_warehouse_id), nullptr);
  // H_DATE
  auto h_date = GetTimeStamp();
  history_tuple->SetValue(5, type::ValueFactory::GetTimestampValue(h_date), nullptr);
  // H_AMOUNT
  history_tuple->SetValue(6, type::ValueFactory::GetDecimalValue(history_init_amount),
                          nullptr);
  // H_DATA
  auto h_data = GetRandomAlphaNumericString(history_data_length);
  history_tuple->SetValue(7, type::ValueFactory::GetVarcharValue(h_data), pool.get());

  return history_tuple;
}

std::unique_ptr<storage::Tuple> BuildOrdersTuple(const int orders_id,
                                                 const int district_id,
                                                 const int warehouse_id,
                                                 const bool new_order,
                                                 const int o_ol_cnt) {
  auto orders_table_schema = orders_table->GetSchema();
  std::unique_ptr<storage::Tuple> orders_tuple(
      new storage::Tuple(orders_table_schema, allocate));

  // O_ID
  orders_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(orders_id), nullptr);
  // O_C_ID
  auto o_c_id = GetRandomInteger(0, state.customers_per_district);
  orders_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(o_c_id), nullptr);
  // O_D_ID
  orders_tuple->SetValue(2, type::ValueFactory::GetIntegerValue(district_id),
                         nullptr);
  // O_W_ID
  orders_tuple->SetValue(3, type::ValueFactory::GetIntegerValue(warehouse_id),
                         nullptr);
  // O_ENTRY_D
  auto o_entry_d = GetTimeStamp();
  orders_tuple->SetValue(4, type::ValueFactory::GetTimestampValue(o_entry_d),
                         nullptr);
  // O_CARRIER_ID
  auto o_carrier_id = orders_null_carrier_id;
  if (new_order == false) {
    o_carrier_id =
        GetRandomInteger(orders_min_carrier_id, orders_max_carrier_id);
  }
  orders_tuple->SetValue(5, type::ValueFactory::GetIntegerValue(o_carrier_id),
                         nullptr);
  // O_OL_CNT
  orders_tuple->SetValue(6, type::ValueFactory::GetIntegerValue(o_ol_cnt), nullptr);
  // O_ALL_LOCAL
  orders_tuple->SetValue(
      7, type::ValueFactory::GetIntegerValue(orders_init_all_local), nullptr);

  return orders_tuple;
}

std::unique_ptr<storage::Tuple> BuildNewOrderTuple(const int orders_id,
                                                   const int district_id,
                                                   const int warehouse_id) {
  auto new_order_table_schema = new_order_table->GetSchema();
  std::unique_ptr<storage::Tuple> new_order_tuple(
      new storage::Tuple(new_order_table_schema, allocate));

  // NO_O_ID
  new_order_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(orders_id),
                            nullptr);
  // NO_D_ID
  new_order_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(district_id),
                            nullptr);
  // NO_W_ID
  new_order_tuple->SetValue(2, type::ValueFactory::GetIntegerValue(warehouse_id),
                            nullptr);

  return new_order_tuple;
}

std::unique_ptr<storage::Tuple> BuildOrderLineTuple(
    const int orders_id, const int district_id, const int warehouse_id,
    const int order_line_id, const int ol_supply_w_id, const bool new_order,
    const std::unique_ptr<type::AbstractPool> &pool) {
  auto order_line_table_schema = order_line_table->GetSchema();
  std::unique_ptr<storage::Tuple> order_line_tuple(
      new storage::Tuple(order_line_table_schema, allocate));

  // OL_O_ID
  order_line_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(orders_id),
                             nullptr);
  // OL_D_ID
  order_line_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(district_id),
                             nullptr);
  // OL_W_ID
  order_line_tuple->SetValue(2, type::ValueFactory::GetIntegerValue(warehouse_id),
                             nullptr);
  // OL_NUMBER
  order_line_tuple->SetValue(3, type::ValueFactory::GetIntegerValue(order_line_id),
                             nullptr);
  // OL_I_ID
  auto ol_i_id = GetRandomInteger(0, state.item_count);
  order_line_tuple->SetValue(4, type::ValueFactory::GetIntegerValue(ol_i_id),
                             nullptr);
  // OL_SUPPLY_W_ID
  order_line_tuple->SetValue(5, type::ValueFactory::GetIntegerValue(ol_supply_w_id),
                             nullptr);
  // OL_DELIVERY_D
  int64_t ol_delivery_d = GetTimeStamp();
  if (new_order == true) {
    ol_delivery_d = type::PELOTON_INT64_MIN;
  }
  order_line_tuple->SetValue(6, type::ValueFactory::GetTimestampValue(ol_delivery_d),
                             nullptr);
  // OL_QUANTITY
  order_line_tuple->SetValue(
      7, type::ValueFactory::GetIntegerValue(order_line_init_quantity), nullptr);
  // OL_AMOUNT
  double ol_amount = 0;
  if (new_order == true) {
    ol_amount = GetRandomDouble(order_line_min_amount,
                                order_line_max_ol_quantity * item_max_price);
  }
  order_line_tuple->SetValue(8, type::ValueFactory::GetDecimalValue(ol_amount),
                             nullptr);
  // OL_DIST_INFO
  auto ol_dist_info = GetRandomAlphaNumericString(order_line_dist_info_length);
  order_line_tuple->SetValue(9, type::ValueFactory::GetVarcharValue(ol_dist_info),
                             pool.get());

  return order_line_tuple;
}

std::unique_ptr<storage::Tuple> BuildStockTuple(
    const int stock_id, const int s_w_id,
    const std::unique_ptr<type::AbstractPool> &pool) {
  auto stock_table_schema = stock_table->GetSchema();
  std::unique_ptr<storage::Tuple> stock_tuple(
      new storage::Tuple(stock_table_schema, allocate));

  // S_I_ID
  stock_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(stock_id), nullptr);
  // S_W_ID
  stock_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(s_w_id), nullptr);
  // S_QUANTITY
  auto s_quantity = GetRandomInteger(stock_min_quantity, stock_max_quantity);
  stock_tuple->SetValue(2, type::ValueFactory::GetIntegerValue(s_quantity), nullptr);
  // S_DIST_01 .. S_DIST_10
  auto s_dist = GetRandomAlphaNumericString(name_length);
  stock_tuple->SetValue(3, type::ValueFactory::GetVarcharValue(s_dist), pool.get());
  stock_tuple->SetValue(4, type::ValueFactory::GetVarcharValue(s_dist), pool.get());
  stock_tuple->SetValue(5, type::ValueFactory::GetVarcharValue(s_dist), pool.get());
  stock_tuple->SetValue(6, type::ValueFactory::GetVarcharValue(s_dist), pool.get());
  stock_tuple->SetValue(7, type::ValueFactory::GetVarcharValue(s_dist), pool.get());
  stock_tuple->SetValue(8, type::ValueFactory::GetVarcharValue(s_dist), pool.get());
  stock_tuple->SetValue(9, type::ValueFactory::GetVarcharValue(s_dist), pool.get());
  stock_tuple->SetValue(10, type::ValueFactory::GetVarcharValue(s_dist), pool.get());
  stock_tuple->SetValue(11, type::ValueFactory::GetVarcharValue(s_dist), pool.get());
  stock_tuple->SetValue(12, type::ValueFactory::GetVarcharValue(s_dist), pool.get());
  // S_YTD
  auto s_ytd = 0;
  stock_tuple->SetValue(13, type::ValueFactory::GetIntegerValue(s_ytd), nullptr);
  // S_ORDER_CNT
  auto s_order_cnt = 0;
  stock_tuple->SetValue(14, type::ValueFactory::GetIntegerValue(s_order_cnt),
                        nullptr);
  // S_REMOTE_CNT
  auto s_remote_cnt = 0;
  stock_tuple->SetValue(15, type::ValueFactory::GetIntegerValue(s_remote_cnt),
                        nullptr);
  // S_DATA
  auto s_data = GetRandomAlphaNumericString(data_length);
  stock_tuple->SetValue(16, type::ValueFactory::GetVarcharValue(s_data), pool.get());

  return stock_tuple;
}

void LoadItems() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  for (auto item_itr = 0; item_itr < state.item_count; item_itr++) {
    auto item_tuple = BuildItemTuple(item_itr, pool);
    planner::InsertPlan node(item_table, std::move(item_tuple));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction(txn);
}

void LoadWarehouses(const int &warehouse_from, const int &warehouse_to) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  std::unique_ptr<executor::ExecutorContext> context;

  // WAREHOUSES
  for (auto warehouse_itr = warehouse_from; warehouse_itr < warehouse_to; warehouse_itr++) {

    std::unique_ptr<type::AbstractPool> pool(new type::EphemeralPool());

    auto txn = txn_manager.BeginTransaction();
    context.reset(new executor::ExecutorContext(txn));

    auto warehouse_tuple = BuildWarehouseTuple(warehouse_itr, pool);
    planner::InsertPlan warehouse_node(warehouse_table,
                                       std::move(warehouse_tuple));
    executor::InsertExecutor warehouse_executor(&warehouse_node, context.get());
    warehouse_executor.Execute();

    txn_manager.CommitTransaction(txn);

    // DISTRICTS
    for (auto district_itr = 0; district_itr < state.districts_per_warehouse;
         district_itr++) {
      auto txn = txn_manager.BeginTransaction();
      context.reset(new executor::ExecutorContext(txn));

      auto district_tuple =
          BuildDistrictTuple(district_itr, warehouse_itr, pool);
      planner::InsertPlan district_node(district_table,
                                        std::move(district_tuple));
      executor::InsertExecutor district_executor(&district_node, context.get());
      district_executor.Execute();

      txn_manager.CommitTransaction(txn);

      // CUSTOMERS
      for (auto customer_itr = 0; customer_itr < state.customers_per_district;
           customer_itr++) {
        auto txn = txn_manager.BeginTransaction();
        context.reset(new executor::ExecutorContext(txn));

        auto customer_tuple =
            BuildCustomerTuple(customer_itr, district_itr, warehouse_itr, pool);
        planner::InsertPlan customer_node(customer_table,
                                          std::move(customer_tuple));
        executor::InsertExecutor customer_executor(&customer_node,
                                                   context.get());
        customer_executor.Execute();

        // HISTORY

        int history_district_id = district_itr;
        int history_warehouse_id = warehouse_itr;
        auto history_tuple =
            BuildHistoryTuple(customer_itr, district_itr, warehouse_itr,
                              history_district_id, history_warehouse_id, pool);
        planner::InsertPlan history_node(history_table,
                                         std::move(history_tuple));
        executor::InsertExecutor history_executor(&history_node, context.get());
        history_executor.Execute();

        txn_manager.CommitTransaction(txn);

      }  // END CUSTOMERS

      // ORDERS
      for (auto orders_itr = 0; orders_itr < state.customers_per_district;
           orders_itr++) {
        auto txn = txn_manager.BeginTransaction();
        context.reset(new executor::ExecutorContext(txn));

        // New order ?
        auto new_order_threshold =
            state.customers_per_district - new_orders_per_district;
        bool new_order = (orders_itr > new_order_threshold);
        auto o_ol_cnt = GetRandomInteger(orders_min_ol_cnt, orders_max_ol_cnt);

        auto orders_tuple = BuildOrdersTuple(
            orders_itr, district_itr, warehouse_itr, new_order, o_ol_cnt);
        planner::InsertPlan orders_node(orders_table, std::move(orders_tuple));
        executor::InsertExecutor orders_executor(&orders_node, context.get());
        orders_executor.Execute();

        // NEW_ORDER
        if (new_order) {
          auto new_order_tuple =
              BuildNewOrderTuple(orders_itr, district_itr, warehouse_itr);
          planner::InsertPlan new_order_node(new_order_table,
                                             std::move(new_order_tuple));
          executor::InsertExecutor new_order_executor(&new_order_node,
                                                      context.get());
          new_order_executor.Execute();
        }

        // ORDER_LINE
        for (auto order_line_itr = 0; order_line_itr < o_ol_cnt;
             order_line_itr++) {
          int ol_supply_w_id = warehouse_itr;
          auto order_line_tuple = BuildOrderLineTuple(
              orders_itr, district_itr, warehouse_itr, order_line_itr,
              ol_supply_w_id, new_order, pool);
          planner::InsertPlan order_line_node(order_line_table,
                                              std::move(order_line_tuple));
          executor::InsertExecutor order_line_executor(&order_line_node,
                                                       context.get());
          order_line_executor.Execute();
        }

        txn_manager.CommitTransaction(txn);
      }

    }  // END DISTRICTS

    // STOCK
    for (auto stock_itr = 0; stock_itr < state.item_count; stock_itr++) {
      auto txn = txn_manager.BeginTransaction();
      context.reset(new executor::ExecutorContext(txn));

      int s_w_id = warehouse_itr;
      auto stock_tuple = BuildStockTuple(stock_itr, s_w_id, pool);
      planner::InsertPlan stock_node(stock_table, std::move(stock_tuple));
      executor::InsertExecutor stock_executor(&stock_node, context.get());
      stock_executor.Execute();

      txn_manager.CommitTransaction(txn);
    }

  }  // END WAREHOUSES
}

void LoadTPCCDatabase() {

  std::chrono::steady_clock::time_point start_time;
  start_time = std::chrono::steady_clock::now();

  LoadItems();

  if (state.warehouse_count < state.loader_count) {
    std::vector<std::unique_ptr<std::thread>> load_threads(state.warehouse_count);
    for (int thread_id = 0; thread_id < state.warehouse_count; ++thread_id) {
      int warehouse_from = thread_id;
      int warehouse_to = thread_id + 1;
      load_threads[thread_id].reset(new std::thread(LoadWarehouses, warehouse_from, warehouse_to));
    }

    for (auto thread_id = 0; thread_id < state.warehouse_count; ++thread_id) {
      load_threads[thread_id]->join();
    }

  } else {
    std::vector<std::unique_ptr<std::thread>> load_threads(state.loader_count);
    int warehouse_per_thread = state.warehouse_count / state.loader_count;
    for (int thread_id = 0; thread_id < state.loader_count - 1; ++thread_id) {
      int warehouse_from = warehouse_per_thread * thread_id;
      int warehouse_to = warehouse_per_thread * (thread_id + 1);
      load_threads[thread_id].reset(new std::thread(LoadWarehouses, warehouse_from, warehouse_to));
    }
    int thread_id = state.loader_count - 1;
    int warehouse_from = warehouse_per_thread * thread_id;
    int warehouse_to = state.warehouse_count;
    load_threads[thread_id].reset(new std::thread(LoadWarehouses, warehouse_from, warehouse_to));

    for (auto thread_id = 0; thread_id < state.loader_count; ++thread_id) {
      load_threads[thread_id]->join();
    }
  }

  std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
  UNUSED_ATTRIBUTE double diff = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  LOG_INFO("database loading time = %lf ms", diff);

  LOG_INFO("%sTABLE SIZES%s", peloton::GETINFO_HALF_THICK_LINE.c_str(), peloton::GETINFO_HALF_THICK_LINE.c_str());
  LOG_INFO("warehouse count = %lu", warehouse_table->GetTupleCount());
  LOG_INFO("district count  = %lu", district_table->GetTupleCount());
  LOG_INFO("item count = %lu", item_table->GetTupleCount());
  LOG_INFO("customer count = %lu", customer_table->GetTupleCount());
  LOG_INFO("history count = %lu", history_table->GetTupleCount());
  LOG_INFO("stock count = %lu", stock_table->GetTupleCount());
  LOG_INFO("orders count = %lu", orders_table->GetTupleCount());
  LOG_INFO("new order count = %lu", new_order_table->GetTupleCount());
  LOG_INFO("order line count = %lu", order_line_table->GetTupleCount());
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

#ifdef __APPLE__
#pragma clang diagnostic pop
#endif