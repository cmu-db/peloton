//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// configuration.h
//
// Identification: benchmark/tpcc/configuration.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "backend/common/types.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

static const oid_t tpcc_database_oid = 100;

static const oid_t warehouse_table_oid = 1001;
static const oid_t warehouse_table_pkey_index_oid = 20010; // W_ID

static const oid_t district_table_oid = 1002;
static const oid_t district_table_pkey_index_oid = 20021; // D_ID, D_W_ID

static const oid_t item_table_oid = 1003;
static const oid_t item_table_pkey_index_oid = 20030; // I_ID

static const oid_t customer_table_oid = 1004;
static const oid_t customer_table_pkey_index_oid = 20040; // C_W_ID, C_D_ID, C_ID
static const oid_t customer_table_skey_index_oid = 20041; // C_W_ID, C_D_ID, C_LAST

static const oid_t history_table_oid = 1005;

static const oid_t stock_table_oid = 1006;
static const oid_t stock_table_pkey_index_oid = 20060; // S_W_ID, S_I_ID

static const oid_t orders_table_oid = 1007;
static const oid_t orders_table_pkey_index_oid = 20070; // O_W_ID, O_D_ID, O_ID
static const oid_t orders_table_skey_index_oid = 20071; // O_W_ID, O_D_ID, O_C_ID

static const oid_t new_order_table_oid = 1008;
static const oid_t new_order_table_pkey_index_oid = 20080; // NO_D_ID, NO_W_ID, NO_O_ID

static const oid_t order_line_table_oid = 1008;
static const oid_t order_line_table_pkey_index_oid = 20080; // OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER
static const oid_t order_line_table_skey_index_oid = 20081; // OL_W_ID, OL_D_ID, OL_O_ID

class configuration {
 public:

  // num of warehouses
  int warehouse_count;

  // number of backends
  int backend_count;

  // scale factor
  int scale_factor;

  // item count
  int item_count;

  // number of transactions
  unsigned long int transaction_count;

  int districts_per_warehouse;

  int customers_per_district;

  int new_orders_per_district;
};

extern configuration state;

void Usage(FILE *out);

void ValidateBackendCount(const configuration &state);

void ValidateWarehouseCount(const configuration &state);

void ValidateTransactionCount(const configuration &state);

void ParseArguments(int argc, char *argv[], configuration &state);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
