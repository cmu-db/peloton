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

#include "backend/benchmark/benchmark_common.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/executor/abstract_executor.h"
#include "backend/storage/data_table.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"

namespace peloton {

namespace storage{
class DataTable;
}

namespace benchmark {
namespace tpcc {

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



extern configuration state;

void RunWorkload();

/////////////////////////////////////////////////////////
// TRANSACTION TYPES
/////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////
struct NewOrderPlans {

  executor::IndexScanExecutor* item_index_scan_executor_;
  executor::IndexScanExecutor* warehouse_index_scan_executor_;
  executor::IndexScanExecutor* district_index_scan_executor_;
  executor::IndexScanExecutor* district_update_index_scan_executor_;
  executor::UpdateExecutor* district_update_executor_;
  executor::IndexScanExecutor* customer_index_scan_executor_;
  executor::IndexScanExecutor* stock_index_scan_executor_;
  executor::IndexScanExecutor* stock_update_index_scan_executor_;
  executor::UpdateExecutor* stock_update_executor_;


  void SetContext(executor::ExecutorContext* context) {
    item_index_scan_executor_->SetContext(context);
    
    warehouse_index_scan_executor_->SetContext(context);
    
    district_index_scan_executor_->SetContext(context);
    district_update_index_scan_executor_->SetContext(context);
    district_update_executor_->SetContext(context);
    
    customer_index_scan_executor_->SetContext(context);
    
    stock_index_scan_executor_->SetContext(context);
    stock_update_index_scan_executor_->SetContext(context);
    stock_update_executor_->SetContext(context);
  }

  void Cleanup() {
    delete item_index_scan_executor_;
    item_index_scan_executor_ = nullptr;

    delete warehouse_index_scan_executor_;
    warehouse_index_scan_executor_ = nullptr;

    delete district_index_scan_executor_;
    district_index_scan_executor_ = nullptr;

    delete district_update_index_scan_executor_;
    district_update_index_scan_executor_ = nullptr;

    delete district_update_executor_;
    district_update_executor_ = nullptr;

    delete customer_index_scan_executor_;
    customer_index_scan_executor_ = nullptr;

    delete stock_index_scan_executor_;
    stock_index_scan_executor_ = nullptr;

    delete stock_update_index_scan_executor_;
    stock_update_index_scan_executor_ = nullptr;

    delete stock_update_executor_;
    stock_update_executor_ = nullptr;
  }

};


struct PaymentPlans {

  executor::IndexScanExecutor* customer_pindex_scan_executor_;
  executor::IndexScanExecutor* customer_index_scan_executor_;
  executor::IndexScanExecutor* customer_update_bc_index_scan_executor_;
  executor::UpdateExecutor* customer_update_bc_executor_;
  executor::IndexScanExecutor* customer_update_gc_index_scan_executor_;
  executor::UpdateExecutor* customer_update_gc_executor_;

  executor::IndexScanExecutor* warehouse_index_scan_executor_;
  executor::IndexScanExecutor* warehouse_update_index_scan_executor_;
  executor::UpdateExecutor* warehouse_update_executor_;
  
  executor::IndexScanExecutor* district_index_scan_executor_;
  executor::IndexScanExecutor* district_update_index_scan_executor_;
  executor::UpdateExecutor* district_update_executor_;

  void SetContext(executor::ExecutorContext* context) {
    customer_pindex_scan_executor_->SetContext(context);
    customer_index_scan_executor_->SetContext(context);
    customer_update_bc_index_scan_executor_->SetContext(context);
    customer_update_bc_executor_->SetContext(context);
    customer_update_gc_index_scan_executor_->SetContext(context);
    customer_update_gc_executor_->SetContext(context);
    
    warehouse_index_scan_executor_->SetContext(context);
    warehouse_update_index_scan_executor_->SetContext(context);
    warehouse_update_executor_->SetContext(context);

    district_index_scan_executor_->SetContext(context);
    district_update_index_scan_executor_->SetContext(context);
    district_update_executor_->SetContext(context);
  }

  void Cleanup() {
    delete customer_pindex_scan_executor_;
    customer_pindex_scan_executor_ = nullptr;
    delete customer_index_scan_executor_;
    customer_index_scan_executor_ = nullptr;
    delete customer_update_bc_index_scan_executor_;
    customer_update_bc_index_scan_executor_ = nullptr;
    delete customer_update_bc_executor_;
    customer_update_bc_executor_ = nullptr;
    delete customer_update_gc_index_scan_executor_;
    customer_update_gc_index_scan_executor_ = nullptr;
    delete customer_update_gc_executor_;
    customer_update_gc_executor_ = nullptr;
    
    delete warehouse_index_scan_executor_;
    warehouse_index_scan_executor_ = nullptr;
    delete warehouse_update_index_scan_executor_;
    warehouse_update_index_scan_executor_ = nullptr;
    delete warehouse_update_executor_;
    warehouse_update_executor_ = nullptr;
    
    delete district_index_scan_executor_;
    district_index_scan_executor_ = nullptr;
    delete district_update_index_scan_executor_;
    district_update_index_scan_executor_ = nullptr;
    delete district_update_executor_;
    district_update_executor_ = nullptr;
  }

};

// struct OrderStatusPlans {

//   executor::IndexScanExecutor* customer_pindex_scan_executor_;
//   executor::IndexScanExecutor* customer_index_scan_executor_;
//   executor::IndexScanExecutor* orders_index_scan_executor_;
//   executor::IndexScanExecutor* order_line_index_scan_executor_;
  

//   void SetContext(executor::ExecutorContext* context) {
//     customer_pindex_scan_executor_->SetContext(context);
//     customer_index_scan_executor_->SetContext(context);
//     orders_index_scan_executor_->SetContext(context);
//     order_line_index_scan_executor_->SetContext(context);
//   }

//   void Cleanup() {
//     delete customer_pindex_scan_executor_;
//     customer_pindex_scan_executor_ = nullptr;

//     delete customer_index_scan_executor_;
//     customer_index_scan_executor_ = nullptr;

//     delete orders_index_scan_executor_;
//     orders_index_scan_executor_ = nullptr;

//     delete order_line_index_scan_executor_;
//     order_line_index_scan_executor_ = nullptr;
//   }
// };



NewOrderPlans PrepareNewOrderPlan();

PaymentPlans PreparePaymentPlan();

// OrderStatusPlans PrepareOrderStatusPlan();

size_t GenerateWarehouseId(const size_t thread_id);

bool RunNewOrder(NewOrderPlans &new_order_plans, const size_t &thread_id);

bool RunPayment(PaymentPlans &payment_plans);

bool RunDelivery();


bool RunOrderStatus();

bool RunStockLevel();




/////////////////////////////////////////////////////////

std::vector<std::vector<Value>>
ExecuteReadTest(executor::AbstractExecutor* executor);

void ExecuteUpdateTest(executor::AbstractExecutor* executor);

void ExecuteDeleteTest(executor::AbstractExecutor* executor);

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton