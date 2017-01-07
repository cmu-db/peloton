//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_tuner_test.cpp
//
// Identification: test/brain/index_tuner_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <cstdio>
#include <random>
#include <chrono>

#include "common/harness.h"

#include "brain/index_tuner.h"
#include "brain/sample.h"
#include "common/generator.h"

#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_tests_util.h"

#include "storage/table_factory.h"
#include "index/index_factory.h"
#include "catalog/catalog.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Index Tuner Tests
//===--------------------------------------------------------------------===//

class IndexTunerTests : public PelotonTest {};

TEST_F(IndexTunerTests, BasicTest) {

  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;

  // Create a table and populate it
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuple_count, false));
  ExecutorTestsUtil::PopulateTable(data_table.get(), tuple_count, false, false,
                                   true, txn);
  txn_manager.CommitTransaction(txn);

  // Check column count
  oid_t column_count = data_table->GetSchema()->GetColumnCount();
  EXPECT_EQ(column_count, 4);

  // Index tuner
  brain::IndexTuner &index_tuner = brain::IndexTuner::GetInstance();

  // Attach table to index tuner
  index_tuner.AddTable(data_table.get());

  // Check old index count
  auto old_index_count = data_table->GetIndexCount();
  EXPECT_TRUE(old_index_count == 0);
  LOG_INFO("Index Count: %u", old_index_count);

  // Start index tuner
  index_tuner.Start();

  std::vector<double> columns_accessed(column_count, 0);
  double sample_weight;

  UniformGenerator generator;
  for (int sample_itr = 0; sample_itr < 10000; sample_itr++) {
    auto rng_val = generator.GetSample();

    if (rng_val < 0.6) {
      columns_accessed = {0, 1, 2};
      sample_weight = 100;
    } else if (rng_val < 0.9) {
      columns_accessed = {0, 2};
      sample_weight = 10;
    } else {
      columns_accessed = {0, 1};
      sample_weight = 10;
    }

    // Create a table access sample
    // Indicates the columns present in predicate, query weight, and selectivity
    brain::Sample sample(columns_accessed,
                         sample_weight,
                         brain::SAMPLE_TYPE_ACCESS);

    // Collect index sample in table
    data_table->RecordIndexSample(sample);

    // Periodically sleep a bit
    // Index tuner thread will process the index samples periodically,
    // and materialize the appropriate ad-hoc indexes
    if(sample_itr % 100 == 0 ){
      std::this_thread::sleep_for(std::chrono::microseconds(10000));
    }
  }

  // Wait for tuner to build indexes
  sleep(5);

  // Stop index tuner
  index_tuner.Stop();

  // Detach all tables from index tuner
  index_tuner.ClearTables();

  // Check new index count
  auto new_index_count = data_table->GetIndexCount();
  LOG_INFO("Index Count: %u", new_index_count);

  EXPECT_TRUE(new_index_count != old_index_count);
  EXPECT_TRUE(new_index_count == 3);

  // Check candidate indices to ensure that
  // all the ad-hoc indexes are materialized
  std::vector<std::set<oid_t>> candidate_indices;
  candidate_indices.push_back({0, 1, 2});
  candidate_indices.push_back({0, 2});
  candidate_indices.push_back({0, 1});

  for (auto candidate_index : candidate_indices) {
    bool candidate_index_found = false;
    oid_t index_itr;
    for (index_itr = 0; index_itr < new_index_count; index_itr++) {
      // Check attributes
      auto index_attrs = data_table->GetIndexAttrs(index_itr);
      if (index_attrs != candidate_index) {
        continue;
      }
      // Exact match
      candidate_index_found = true;
      break;
    }

    EXPECT_TRUE(candidate_index_found);
  }

}

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


static const oid_t tpcc_database_oid = 100;

static const oid_t warehouse_table_oid = 1001;
static const oid_t warehouse_table_pkey_index_oid = 20010;  // W_ID

static const oid_t district_table_oid = 1002;
static const oid_t district_table_pkey_index_oid = 20021;  // D_ID, D_W_ID

static const oid_t item_table_oid = 1003;
static const oid_t item_table_pkey_index_oid = 20030;  // I_ID

static const oid_t customer_table_oid = 1004;
static const oid_t customer_table_pkey_index_oid =
    20040;  // C_W_ID, C_D_ID, C_ID
static const oid_t customer_table_skey_index_oid =
    20041;  // C_W_ID, C_D_ID, C_LAST

static const oid_t history_table_oid = 1005;

static const oid_t stock_table_oid = 1006;
static const oid_t stock_table_pkey_index_oid = 20060;  // S_W_ID, S_I_ID

static const oid_t orders_table_oid = 1007;
static const oid_t orders_table_pkey_index_oid = 20070;  // O_W_ID, O_D_ID, O_ID
static const oid_t orders_table_skey_index_oid =
    20071;  // O_W_ID, O_D_ID, O_C_ID

static const oid_t new_order_table_oid = 1008;
static const oid_t new_order_table_pkey_index_oid =
    20080;  // NO_D_ID, NO_W_ID, NO_O_ID

static const oid_t order_line_table_oid = 1008;
static const oid_t order_line_table_pkey_index_oid =
    20080;  // OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER
static const oid_t order_line_table_skey_index_oid =
    20081;  // OL_W_ID, OL_D_ID, OL_O_ID

//===========
// Column ids
//===========

// NEW_ORDER
#define COL_IDX_NO_O_ID       0
#define COL_IDX_NO_D_ID       1
#define COL_IDX_NO_W_ID       2

// ORDERS
#define COL_IDX_O_ID          0
#define COL_IDX_O_C_ID        1
#define COL_IDX_O_D_ID        2
#define COL_IDX_O_W_ID        3
#define COL_IDX_O_ENTRY_D     4
#define COL_IDX_O_CARRIER_ID  5
#define COL_IDX_O_OL_CNT      6
#define COL_IDX_O_ALL_LOCAL   7

// ORDER_LINE
#define COL_IDX_OL_O_ID       0
#define COL_IDX_OL_D_ID       1
#define COL_IDX_OL_W_ID       2
#define COL_IDX_OL_NUMBER     3
#define COL_IDX_OL_I_ID       4
#define COL_IDX_OL_SUPPLY_W_ID      5
#define COL_IDX_OL_DELIVERY_D 6
#define COL_IDX_OL_QUANTITY   7
#define COL_IDX_OL_AMOUNT     8
#define COL_IDX_OL_DIST_INFO  9

// Customer
#define COL_IDX_C_ID              0
#define COL_IDX_C_D_ID            1
#define COL_IDX_C_W_ID            2
#define COL_IDX_C_FIRST           3
#define COL_IDX_C_MIDDLE          4
#define COL_IDX_C_LAST            5
#define COL_IDX_C_STREET_1        6
#define COL_IDX_C_STREET_2        7
#define COL_IDX_C_CITY            8
#define COL_IDX_C_STATE           9
#define COL_IDX_C_ZIP             10
#define COL_IDX_C_PHONE           11
#define COL_IDX_C_SINCE           12
#define COL_IDX_C_CREDIT          13
#define COL_IDX_C_CREDIT_LIM      14
#define COL_IDX_C_DISCOUNT        15
#define COL_IDX_C_BALANCE         16
#define COL_IDX_C_YTD_PAYMENT     17
#define COL_IDX_C_PAYMENT_CNT     18
#define COL_IDX_C_DELIVERY_CNT    19
#define COL_IDX_C_DATA            20

// District
#define COL_IDX_D_ID              0
#define COL_IDX_D_W_ID            1
#define COL_IDX_D_NAME            2
#define COL_IDX_D_STREET_1        3
#define COL_IDX_D_STREET_2        4
#define COL_IDX_D_CITY            5
#define COL_IDX_D_STATE           6
#define COL_IDX_D_ZIP             7
#define COL_IDX_D_TAX             8
#define COL_IDX_D_YTD             9
#define COL_IDX_D_NEXT_O_ID       10

// Stock
#define COL_IDX_S_I_ID            0
#define COL_IDX_S_W_ID            1
#define COL_IDX_S_QUANTITY        2
#define COL_IDX_S_DIST_01         3
#define COL_IDX_S_DIST_02         4
#define COL_IDX_S_DIST_03         5
#define COL_IDX_S_DIST_04         6
#define COL_IDX_S_DIST_05         7
#define COL_IDX_S_DIST_06         8
#define COL_IDX_S_DIST_07         9
#define COL_IDX_S_DIST_08         10
#define COL_IDX_S_DIST_09         11
#define COL_IDX_S_DIST_10         12
#define COL_IDX_S_YTD             13
#define COL_IDX_S_ORDER_CNT       14
#define COL_IDX_S_REMOTE_CNT      15
#define COL_IDX_S_DATA            16



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
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "W_ID", is_inlined);
  warehouse_columns.push_back(w_id_column);
  auto w_name_column = catalog::Column(
      type::Type::VARCHAR, warehouse_name_length, "W_NAME", is_inlined);
  warehouse_columns.push_back(w_name_column);
  auto w_street_1_column = catalog::Column(type::Type::VARCHAR, street_length,
                                           "W_STREET_1", is_inlined);
  warehouse_columns.push_back(w_street_1_column);
  auto w_street_2_column = catalog::Column(type::Type::VARCHAR, street_length,
                                           "W_STREET_2", is_inlined);
  warehouse_columns.push_back(w_street_2_column);
  auto w_city_column =
      catalog::Column(type::Type::VARCHAR, city_length, "W_CITY", is_inlined);
  warehouse_columns.push_back(w_city_column);
  auto w_state_column =
      catalog::Column(type::Type::VARCHAR, state_length, "W_STATE", is_inlined);
  warehouse_columns.push_back(w_state_column);
  auto w_zip_column =
      catalog::Column(type::Type::VARCHAR, zip_length, "W_ZIP", is_inlined);
  warehouse_columns.push_back(w_zip_column);
  auto w_tax_column = catalog::Column(
      type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL), "W_TAX", is_inlined);
  warehouse_columns.push_back(w_tax_column);
  auto w_ytd_column = catalog::Column(
      type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL), "W_YTD", is_inlined);
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
    tpcc_database_oid, INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
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
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER), "D_ID", is_inlined);
  district_columns.push_back(d_id_column);
  auto d_w_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "D_W_ID", is_inlined);
  district_columns.push_back(d_w_id_column);
  auto d_name_column = catalog::Column(type::Type::VARCHAR, district_name_length,
                                       "D_NAME", is_inlined);
  district_columns.push_back(d_name_column);
  auto d_street_1_column = catalog::Column(type::Type::VARCHAR, street_length,
                                           "D_STREET_1", is_inlined);
  district_columns.push_back(d_street_1_column);
  auto d_street_2_column = catalog::Column(type::Type::VARCHAR, street_length,
                                           "D_STREET_2", is_inlined);
  district_columns.push_back(d_street_2_column);
  auto d_city_column =
      catalog::Column(type::Type::VARCHAR, city_length, "D_CITY", is_inlined);
  district_columns.push_back(d_city_column);
  auto d_state_column =
      catalog::Column(type::Type::VARCHAR, state_length, "D_STATE", is_inlined);
  district_columns.push_back(d_state_column);
  auto d_zip_column =
      catalog::Column(type::Type::VARCHAR, zip_length, "D_ZIP", is_inlined);
  district_columns.push_back(d_zip_column);
  auto d_tax_column = catalog::Column(
      type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL), "D_TAX", is_inlined);
  district_columns.push_back(d_tax_column);
  auto d_ytd_column = catalog::Column(
      type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL), "D_YTD", is_inlined);
  district_columns.push_back(d_ytd_column);
  auto d_next_o_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
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
      district_table_pkey_index_oid, district_table_oid, INDEX_TYPE_BWTREE,
      INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
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
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER), "I_ID", is_inlined);
  item_columns.push_back(i_id_column);
  auto i_im_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "I_IM_ID", is_inlined);
  item_columns.push_back(i_im_id_column);
  auto i_name_column =
      catalog::Column(type::Type::VARCHAR, name_length, "I_NAME", is_inlined);
  item_columns.push_back(i_name_column);
  auto i_price_column = catalog::Column(
      type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL), "I_PRICE", is_inlined);
  item_columns.push_back(i_price_column);
  auto i_data_column =
      catalog::Column(type::Type::VARCHAR, data_length, "I_DATA", is_inlined);
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
      INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema,
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
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER), "C_ID", is_inlined);
  customer_columns.push_back(c_id_column);
  auto c_d_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "C_D_ID", is_inlined);
  customer_columns.push_back(c_d_id_column);
  auto c_w_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "C_W_ID", is_inlined);
  customer_columns.push_back(c_w_id_column);
  auto c_first_name_column =
      catalog::Column(type::Type::VARCHAR, name_length, "C_FIRST", is_inlined);
  customer_columns.push_back(c_first_name_column);
  auto c_middle_name_column = catalog::Column(
      type::Type::VARCHAR, middle_name_length, "C_MIDDLE", is_inlined);
  customer_columns.push_back(c_middle_name_column);
  auto c_last_name_column =
      catalog::Column(type::Type::VARCHAR, name_length, "C_LAST", is_inlined);
  customer_columns.push_back(c_last_name_column);
  auto c_street_1_column = catalog::Column(type::Type::VARCHAR, street_length,
                                           "C_STREET_1", is_inlined);
  customer_columns.push_back(c_street_1_column);
  auto c_street_2_column = catalog::Column(type::Type::VARCHAR, street_length,
                                           "C_STREET_2", is_inlined);
  customer_columns.push_back(c_street_2_column);
  auto c_city_column =
      catalog::Column(type::Type::VARCHAR, city_length, "C_CITY", is_inlined);
  customer_columns.push_back(c_city_column);
  auto c_state_column =
      catalog::Column(type::Type::VARCHAR, state_length, "C_STATE", is_inlined);
  customer_columns.push_back(c_state_column);
  auto c_zip_column =
      catalog::Column(type::Type::VARCHAR, zip_length, "C_ZIP", is_inlined);
  customer_columns.push_back(c_zip_column);
  auto c_phone_column =
      catalog::Column(type::Type::VARCHAR, phone_length, "C_PHONE", is_inlined);
  customer_columns.push_back(c_phone_column);
  auto c_since_column =
      catalog::Column(type::Type::TIMESTAMP, type::Type::GetTypeSize(type::Type::TIMESTAMP),
                      "C_SINCE", is_inlined);
  customer_columns.push_back(c_since_column);
  auto c_credit_column = catalog::Column(type::Type::VARCHAR, credit_length,
                                         "C_CREDIT", is_inlined);
  customer_columns.push_back(c_credit_column);
  auto c_credit_lim_column =
      catalog::Column(type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL),
                      "C_CREDIT_LIM", is_inlined);
  customer_columns.push_back(c_credit_lim_column);
  auto c_discount_column =
      catalog::Column(type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL),
                      "C_DISCOUNT", is_inlined);
  customer_columns.push_back(c_discount_column);
  auto c_balance_column =
      catalog::Column(type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL),
                      "C_BALANCE", is_inlined);
  customer_columns.push_back(c_balance_column);
  auto c_ytd_payment_column =
      catalog::Column(type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL),
                      "C_YTD_PAYMENT", is_inlined);
  customer_columns.push_back(c_ytd_payment_column);
  auto c_payment_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "C_PAYMENT_CNT", is_inlined);
  customer_columns.push_back(c_payment_column);
  auto c_delivery_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "C_DELIVERY_CNT", is_inlined);
  customer_columns.push_back(c_delivery_column);
  auto c_data_column =
      catalog::Column(type::Type::VARCHAR, data_length, "C_DATA", is_inlined);
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
    tpcc_database_oid, INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
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
      tpcc_database_oid, INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_INVALID,
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
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "H_C_ID", is_inlined);
  history_columns.push_back(h_c_id_column);
  auto h_c_d_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "H_C_D_ID", is_inlined);
  history_columns.push_back(h_c_d_id_column);
  auto h_c_w_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "H_C_W_ID", is_inlined);
  history_columns.push_back(h_c_w_id_column);
  auto h_d_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "H_D_ID", is_inlined);
  history_columns.push_back(h_d_id_column);
  auto h_w_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "H_W_ID", is_inlined);
  history_columns.push_back(h_w_id_column);
  auto h_date_column =
      catalog::Column(type::Type::TIMESTAMP, type::Type::GetTypeSize(type::Type::TIMESTAMP),
                      "H_DATE", is_inlined);
  history_columns.push_back(h_date_column);
  auto h_amount_column =
      catalog::Column(type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL),
                      "H_AMOUNT", is_inlined);
  history_columns.push_back(h_amount_column);
  auto h_data_column = catalog::Column(type::Type::VARCHAR, history_data_length,
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
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "S_I_ID", is_inlined);
  stock_columns.push_back(s_i_id_column);
  auto s_w_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "S_W_ID", is_inlined);
  stock_columns.push_back(s_w_id_column);
  auto s_quantity_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "S_QUANTITY", is_inlined);
  stock_columns.push_back(s_quantity_column);
  auto s_dist_01_column =
      catalog::Column(type::Type::VARCHAR, dist_length, "S_DIST_01", is_inlined);
  stock_columns.push_back(s_dist_01_column);
  auto s_dist_02_column =
      catalog::Column(type::Type::VARCHAR, dist_length, "S_DIST_02", is_inlined);
  stock_columns.push_back(s_dist_02_column);
  auto s_dist_03_column =
      catalog::Column(type::Type::VARCHAR, dist_length, "S_DIST_03", is_inlined);
  stock_columns.push_back(s_dist_03_column);
  auto s_dist_04_column =
      catalog::Column(type::Type::VARCHAR, dist_length, "S_DIST_04", is_inlined);
  stock_columns.push_back(s_dist_04_column);
  auto s_dist_05_column =
      catalog::Column(type::Type::VARCHAR, dist_length, "S_DIST_05", is_inlined);
  stock_columns.push_back(s_dist_05_column);
  auto s_dist_06_column =
      catalog::Column(type::Type::VARCHAR, dist_length, "S_DIST_06", is_inlined);
  stock_columns.push_back(s_dist_06_column);
  auto s_dist_07_column =
      catalog::Column(type::Type::VARCHAR, dist_length, "S_DIST_07", is_inlined);
  stock_columns.push_back(s_dist_07_column);
  auto s_dist_08_column =
      catalog::Column(type::Type::VARCHAR, dist_length, "S_DIST_08", is_inlined);
  stock_columns.push_back(s_dist_08_column);
  auto s_dist_09_column =
      catalog::Column(type::Type::VARCHAR, dist_length, "S_DIST_09", is_inlined);
  stock_columns.push_back(s_dist_09_column);
  auto s_dist_10_column =
      catalog::Column(type::Type::VARCHAR, dist_length, "S_DIST_10", is_inlined);
  stock_columns.push_back(s_dist_10_column);
  auto s_ytd_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER), "S_YTD", is_inlined);
  stock_columns.push_back(s_ytd_column);
  auto s_order_cnt_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "S_ORDER_CNT", is_inlined);
  stock_columns.push_back(s_order_cnt_column);
  auto s_discount_cnt_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "S_REMOTE_CNT", is_inlined);
  stock_columns.push_back(s_discount_cnt_column);
  auto s_data_column =
      catalog::Column(type::Type::VARCHAR, data_length, "S_DATA", is_inlined);
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
      tpcc_database_oid, INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
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
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER), "O_ID", is_inlined);
  orders_columns.push_back(o_id_column);
  auto o_c_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "O_C_ID", is_inlined);
  orders_columns.push_back(o_c_id_column);
  auto o_d_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "O_D_ID", is_inlined);
  orders_columns.push_back(o_d_id_column);
  auto o_w_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "O_W_ID", is_inlined);
  orders_columns.push_back(o_w_id_column);
  auto o_entry_d_column =
      catalog::Column(type::Type::TIMESTAMP, type::Type::GetTypeSize(type::Type::TIMESTAMP),
                      "O_ENTRY_D", is_inlined);
  orders_columns.push_back(o_entry_d_column);
  auto o_carrier_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "O_CARRIER_ID", is_inlined);
  orders_columns.push_back(o_carrier_id_column);
  auto o_ol_cnt_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "O_OL_CNT", is_inlined);
  orders_columns.push_back(o_ol_cnt_column);
  auto o_all_local_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
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
      tpcc_database_oid, INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
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
      tpcc_database_oid, INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_INVALID,
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
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "NO_O_ID", is_inlined);
  new_order_columns.push_back(no_o_id_column);
  auto no_d_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "NO_D_ID", is_inlined);
  new_order_columns.push_back(no_d_id_column);
  auto no_w_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
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
    tpcc_database_oid, INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
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
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "OL_O_ID", is_inlined);
  order_line_columns.push_back(ol_o_id_column);
  auto ol_d_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "OL_D_ID", is_inlined);
  order_line_columns.push_back(ol_d_id_column);
  auto ol_w_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "OL_W_ID", is_inlined);
  order_line_columns.push_back(ol_w_id_column);
  auto ol_number_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "OL_NUMBER", is_inlined);
  order_line_columns.push_back(ol_number_column);
  auto ol_i_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "OL_I_ID", is_inlined);
  order_line_columns.push_back(ol_i_id_column);
  auto ol_supply_w_id_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "OL_SUPPLY_W_ID", is_inlined);
  order_line_columns.push_back(ol_supply_w_id_column);
  auto ol_delivery_d_column =
      catalog::Column(type::Type::TIMESTAMP, type::Type::GetTypeSize(type::Type::TIMESTAMP),
                      "OL_DELIVERY_D", is_inlined);
  order_line_columns.push_back(ol_delivery_d_column);
  auto ol_quantity_column =
      catalog::Column(type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
                      "OL_QUANTITY", is_inlined);
  order_line_columns.push_back(ol_quantity_column);
  auto ol_amount_column =
      catalog::Column(type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL),
                      "OL_AMOUNT", is_inlined);
  order_line_columns.push_back(ol_amount_column);
  auto ol_dist_info_column =
      catalog::Column(type::Type::VARCHAR, order_line_dist_info_length,
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
      tpcc_database_oid, INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
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
      tpcc_database_oid, INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_INVALID,
      tuple_schema, key_schema, key_attrs, false);

  std::shared_ptr<index::Index> skey_index(
      index::IndexFactory::GetIndex(index_metadata));
  order_line_table->AddIndex(skey_index);
}

void CreateTPCCDatabase() {
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
  std::string tpcc_database_name = "TPCC";
  tpcc_database = new storage::Database(tpcc_database_oid);
  catalog->AddDatabase(tpcc_database_name, tpcc_database);

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

void DropTPCCDatabase() {

  // Clean up
  delete tpcc_database;

}

TEST_F(IndexTunerTests, TPCCTest) {

  // Create TPCC database
  CreateTPCCDatabase();

  // Index tuner
  brain::IndexTuner &index_tuner = brain::IndexTuner::GetInstance();

  // Set duration between pauses
  auto duration = 10000; // in ms
  index_tuner.SetDurationOfPause(duration);

  // Start index tuner
  index_tuner.Start();

  // Bootstrap TPCC
  index_tuner.BootstrapTPCC();

  // Wait for tuner to build indexes
  sleep(5);

  // Stop index tuner
  index_tuner.Stop();

  // Drop TPCC database
  DropTPCCDatabase();

}

}  // End test namespace
}  // End peloton namespace
