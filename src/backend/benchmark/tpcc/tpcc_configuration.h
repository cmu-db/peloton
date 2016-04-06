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

static const oid_t user_table_oid = 1001;
static const oid_t user_table_pkey_index_oid = 2001;

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
  unsigned long transaction_count;

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
