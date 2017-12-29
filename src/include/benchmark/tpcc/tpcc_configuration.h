//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_configuration.h
//
// Identification: src/include/benchmark/tpcc/tpcc_configuration.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <cstring>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "common/internal_types.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

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


class configuration {
 public:

  // index type
  IndexType index;

  // epoch type
  EpochType epoch;

  // scale factor
  double scale_factor;

  // execution duration (in s)
  double duration;

  // profile duration (in s)
  double profile_duration;

  // number of backends
  int backend_count;

  // num of warehouses
  int warehouse_count;

  // item count
  int item_count;

  int districts_per_warehouse;

  int customers_per_district;

  int new_orders_per_district;

  // exponential backoff
  bool exp_backoff;

  // client affinity
  bool affinity;

  // garbage collection
  bool gc_mode;

  // number of gc threads
  bool gc_backend_count;

  // number of loaders
  int loader_count;

  // throughput
  double throughput = 0;

  // abort rate
  double abort_rate = 0;

  std::vector<double> profile_throughput;

  std::vector<double> profile_abort_rate;

  std::vector<int> profile_memory;
  
};

extern configuration state;

void Usage(FILE *out);

void ParseArguments(int argc, char *argv[], configuration &state);

void ValidateIndex(const configuration &state);

void ValidateScaleFactor(const configuration &state);

void ValidateDuration(const configuration &state);

void ValidateProfileDuration(const configuration &state);

void ValidateBackendCount(const configuration &state);

void ValidateWarehouseCount(const configuration &state);

void ValidateGCBackendCount(const configuration &state);

void WriteOutput();

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
