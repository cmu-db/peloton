//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/tpcc/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "backend/benchmark/tpcc/tpcc_configuration.h"

namespace peloton {
namespace storage{
class Database;
class DataTable;
class Tuple;
}

class VarlenPool;

namespace benchmark {
namespace tpcc {

extern configuration state;

void CreateTPCCDatabase();

void LoadTPCCDatabase();

/////////////////////////////////////////////////////////
// Tables
/////////////////////////////////////////////////////////

extern storage::Database* tpcc_database;
extern storage::DataTable* warehouse_table;
extern storage::DataTable* district_table;
extern storage::DataTable* item_table;
extern storage::DataTable* customer_table;
extern storage::DataTable* history_table;
extern storage::DataTable* stock_table;
extern storage::DataTable* orders_table;
extern storage::DataTable* new_order_table;
extern storage::DataTable* order_line_table;

/////////////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////////////

extern size_t name_length;
extern size_t middle_name_length ;
extern size_t data_length;
extern size_t state_length;
extern size_t zip_length;
extern size_t street_length;
extern size_t city_length;
extern size_t credit_length;
extern size_t phone_length;
extern size_t dist_length;

extern double item_min_price;
extern double item_max_price;

extern double warehouse_name_length;
extern double warehouse_min_tax;
extern double warehouse_max_tax;
extern double warehouse_initial_ytd;

extern double district_name_length;
extern double district_min_tax;
extern double district_max_tax;
extern double district_initial_ytd;

extern std::string customers_good_credit;
extern std::string customers_bad_credit;
extern double customers_bad_credit_ratio;
extern double customers_init_credit_lim;
extern double customers_min_discount;
extern double customers_max_discount;
extern double customers_init_balance;
extern double customers_init_ytd;
extern int customers_init_payment_cnt;
extern int customers_init_delivery_cnt;

extern double history_init_amount;
extern size_t history_data_length;

extern int orders_min_ol_cnt;
extern int orders_max_ol_cnt;
extern int orders_init_all_local;
extern int orders_null_carrier_id;
extern int orders_min_carrier_id;
extern int orders_max_carrier_id;

extern int new_orders_per_district;

extern int order_line_init_quantity;
extern int order_line_max_ol_quantity;
extern double order_line_min_amount;
extern size_t order_line_dist_info_length;

extern double stock_original_ratio;
extern int stock_min_quantity;
extern int stock_max_quantity;
extern int stock_dist_count;

extern double payment_min_amount;
extern double payment_max_amount;

extern int stock_min_threshold;
extern int stock_max_threshold;

extern double new_order_remote_txns;

/////////////////////////////////////////////////////////
// Tuple Constructors
/////////////////////////////////////////////////////////

std::unique_ptr<storage::Tuple>
BuildItemTuple(const int item_id,
               const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple>
BuildWarehouseTuple(const int warehouse_id,
                    const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple>
BuildDistrictTuple(const int district_id,
                   const int warehouse_id,
                   const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple>
BuildCustomerTuple(const int customer_id,
                   const int district_id,
                   const int warehouse_id,
                   const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple>
BuildHistoryTuple(const int customer_id,
                  const int district_id,
                  const int warehouse_id,
                  const int history_district_id,
                  const int history_warehouse_id,
                  const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple>
BuildOrdersTuple(const int orders_id,
                 const int district_id,
                 const int warehouse_id,
                 const bool new_order,
                 const int o_ol_cnt);

std::unique_ptr<storage::Tuple>
BuildNewOrderTuple(const int orders_id,
                   const int district_id,
                   const int warehouse_id);

std::unique_ptr<storage::Tuple>
BuildOrderLineTuple(const int orders_id,
                    const int district_id,
                    const int warehouse_id,
                    const int order_line_id,
                    const int ol_supply_w_id,
                    const bool new_order,
                    const std::unique_ptr<VarlenPool>& pool);

std::unique_ptr<storage::Tuple>
BuildStockTuple(const int stock_id,
                const int s_w_id,
                const std::unique_ptr<VarlenPool>& pool);

/////////////////////////////////////////////////////////
// Utils
/////////////////////////////////////////////////////////

std::string GetRandomAlphaNumericString(const size_t string_length);

bool GetRandomBoolean(double ratio);

int GetRandomInteger(const int lower_bound,
                     const int upper_bound);

int GetRandomIntegerExcluding(const int lower_bound,
                              const int upper_bound,
                              const int exclude_sample);

double GetRandomDouble(const double lower_bound, const double upper_bound);

std::string GetStreetName();

std::string GetZipCode();

std::string GetCityName();

std::string GetStateName();

int GetTimeStamp();

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton
