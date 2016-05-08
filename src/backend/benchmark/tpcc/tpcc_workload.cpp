//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/tpcc/workload.cpp
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
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>


#include "backend/benchmark/tpcc/tpcc_workload.h"
#include "backend/benchmark/tpcc/tpcc_configuration.h"
#include "backend/benchmark/tpcc/tpcc_loader.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/common/generator.h"

#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/container_tuple.h"

#include "backend/index/index_factory.h"

#include "backend/logging/log_manager.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/index_scan_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace tpcc {

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;

void RunBackend(oid_t thread_id) {

  UniformGenerator generator;

  oid_t &execution_count_ref = abort_counts[thread_id];
  oid_t &transaction_count_ref = commit_counts[thread_id];

  // Run these many transactions
  while (true) {
    if (is_running == false) {
      break;
    }
    
    while (RunDelivery() == false) {
      execution_count_ref++;
    }
    
    // auto rng_val = generator.GetSample();
    
    // if (rng_val <= 0.04) {
    //   while (RunStockLevel() == false) {
    //     execution_count_ref++;
    //   }
    // } else if (rng_val <= 0.08) {
    //   while (RunDelivery() == false) {
    //     execution_count_ref++;
    //   }
    // } else if (rng_val <= 0.12) {
    //   while (RunOrderStatus() == false) {
    //     execution_count_ref++;
    //   }
    // } else if (rng_val <= 0.55) {
    //   while (RunPayment() == false) {
    //     execution_count_ref++;
    //   }
    // } else {
    //   while (RunNewOrder() == false) {
    //     execution_count_ref++;
    //   }
    // }

    transaction_count_ref++;

  }
}

void RunWorkload() {

  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  
  abort_counts = new oid_t[num_threads];
  memset(abort_counts, 0, sizeof(oid_t) * num_threads);

  commit_counts = new oid_t[num_threads];
  memset(commit_counts, 0, sizeof(oid_t) * num_threads);

  size_t snapshot_round = (size_t)(state.duration / state.snapshot_duration);

  oid_t **abort_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    abort_counts_snapshots[round_id] = new oid_t[num_threads];
  }

  oid_t **commit_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    commit_counts_snapshots[round_id] = new oid_t[num_threads];
  }

  // Launch a group of threads
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
  }

  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(int(state.snapshot_duration * 1000)));
    memcpy(abort_counts_snapshots[round_id], abort_counts,
           sizeof(oid_t) * num_threads);
    memcpy(commit_counts_snapshots[round_id], commit_counts,
           sizeof(oid_t) * num_threads);
  }

  is_running = false;

  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // calculate the throughput and abort rate for the first round.
  oid_t total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[0][i];
  }

  oid_t total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[0][i];
  }

  state.snapshot_throughput
      .push_back(total_commit_count * 1.0 / state.snapshot_duration);
  state.snapshot_abort_rate
      .push_back(total_abort_count * 1.0 / total_commit_count);

  // calculate the throughput and abort rate for the remaining rounds.
  for (size_t round_id = 0; round_id < snapshot_round - 1; ++round_id) {
    total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_commit_count += commit_counts_snapshots[round_id + 1][i] -
                            commit_counts_snapshots[round_id][i];
    }

    total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_abort_count += abort_counts_snapshots[round_id + 1][i] -
                           abort_counts_snapshots[round_id][i];
    }

    state.snapshot_throughput
        .push_back(total_commit_count * 1.0 / state.snapshot_duration);
    state.snapshot_abort_rate
        .push_back(total_abort_count * 1.0 / total_commit_count);
  }

  // calculate the aggregated throughput and abort rate.
  total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[snapshot_round - 1][i];
  }

  total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[snapshot_round - 1][i];
  }

  state.throughput = total_commit_count * 1.0 / state.duration;
  state.abort_rate = total_abort_count * 1.0 / total_commit_count;

  // cleanup everything.
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] abort_counts_snapshots[round_id];
    abort_counts_snapshots[round_id] = nullptr;
  }

  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] commit_counts_snapshots[round_id];
    commit_counts_snapshots[round_id] = nullptr;
  }
  delete[] abort_counts_snapshots;
  abort_counts_snapshots = nullptr;
  delete[] commit_counts_snapshots;
  commit_counts_snapshots = nullptr;

  delete[] abort_counts;
  abort_counts = nullptr;
  delete[] commit_counts;
  commit_counts = nullptr;


  
  printf("warehouse count = %u\n", warehouse_table->GetAllActiveTupleCount());
  printf("district count  = %u\n", district_table->GetAllActiveTupleCount());
  printf("item count = %u\n", item_table->GetAllActiveTupleCount());
  printf("customer count = %u\n", customer_table->GetAllActiveTupleCount());
  printf("history count = %u\n", history_table->GetAllActiveTupleCount());
  printf("stock count = %u\n", stock_table->GetAllActiveTupleCount());
  printf("orders count = %u\n", orders_table->GetAllActiveTupleCount());
  printf("new order count = %u\n", new_order_table->GetAllActiveTupleCount());
  printf("order line count = %u\n", order_line_table->GetAllActiveTupleCount());
}


/////////////////////////////////////////////////////////
// TABLES
/////////////////////////////////////////////////////////

/*
   CREATE TABLE WAREHOUSE (
   0 W_ID SMALLINT DEFAULT '0' NOT NULL,
   1 W_NAME VARCHAR(16) DEFAULT NULL,
   2 W_STREET_1 VARCHAR(32) DEFAULT NULL,
   3 W_STREET_2 VARCHAR(32) DEFAULT NULL,
   4 W_CITY VARCHAR(32) DEFAULT NULL,
   5 W_STATE VARCHAR(2) DEFAULT NULL,
   6 W_ZIP VARCHAR(9) DEFAULT NULL,
   7 W_TAX FLOAT DEFAULT NULL,
   8 W_YTD FLOAT DEFAULT NULL,
   CONSTRAINT W_PK_ARRAY PRIMARY KEY (W_ID)
   );
   INDEXES:
   0 W_ID
 */

/*
   CREATE TABLE DISTRICT (
   0 D_ID TINYINT DEFAULT '0' NOT NULL,
   1 D_W_ID SMALLINT DEFAULT '0' NOT NULL REFERENCES WAREHOUSE (W_ID),
   2 D_NAME VARCHAR(16) DEFAULT NULL,
   3 D_STREET_1 VARCHAR(32) DEFAULT NULL,
   4 D_STREET_2 VARCHAR(32) DEFAULT NULL,
   5 D_CITY VARCHAR(32) DEFAULT NULL,
   6 D_STATE VARCHAR(2) DEFAULT NULL,
   7 D_ZIP VARCHAR(9) DEFAULT NULL,
   8 D_TAX FLOAT DEFAULT NULL,
   9 D_YTD FLOAT DEFAULT NULL,
   10 D_NEXT_O_ID INT DEFAULT NULL,
   PRIMARY KEY (D_W_ID,D_ID)
   );
   INDEXES:
   0, 1 D_ID, D_W_ID
 */

/*
   CREATE TABLE ITEM (
   0 I_ID INTEGER DEFAULT '0' NOT NULL,
   1 I_IM_ID INTEGER DEFAULT NULL,
   2 I_NAME VARCHAR(32) DEFAULT NULL,
   3 I_PRICE FLOAT DEFAULT NULL,
   4 I_DATA VARCHAR(64) DEFAULT NULL,
   CONSTRAINT I_PK_ARRAY PRIMARY KEY (I_ID)
   );
   INDEXES:
   0 I_ID
 */
/*
     CREATE TABLE CUSTOMER (
     0  C_ID INTEGER DEFAULT '0' NOT NULL,
     1  C_D_ID TINYINT DEFAULT '0' NOT NULL,
     2  C_W_ID SMALLINT DEFAULT '0' NOT NULL,
     3  C_FIRST VARCHAR(32) DEFAULT NULL,
     4  C_MIDDLE VARCHAR(2) DEFAULT NULL,
     5  C_LAST VARCHAR(32) DEFAULT NULL,
     6  C_STREET_1 VARCHAR(32) DEFAULT NULL,
     7  C_STREET_2 VARCHAR(32) DEFAULT NULL,
     8  C_CITY VARCHAR(32) DEFAULT NULL,
     9  C_STATE VARCHAR(2) DEFAULT NULL,
     10 C_ZIP VARCHAR(9) DEFAULT NULL,
     11 C_PHONE VARCHAR(32) DEFAULT NULL,
     12 C_SINCE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
     13 C_CREDIT VARCHAR(2) DEFAULT NULL,
     14 C_CREDIT_LIM FLOAT DEFAULT NULL,
     15 C_DISCOUNT FLOAT DEFAULT NULL,
     16 C_BALANCE FLOAT DEFAULT NULL,
     17 C_YTD_PAYMENT FLOAT DEFAULT NULL,
     18 C_PAYMENT_CNT INTEGER DEFAULT NULL,
     19 C_DELIVERY_CNT INTEGER DEFAULT NULL,
     20 C_DATA VARCHAR(500),
     PRIMARY KEY (C_W_ID,C_D_ID,C_ID),
     UNIQUE (C_W_ID,C_D_ID,C_LAST,C_FIRST),
     CONSTRAINT C_FKEY_D FOREIGN KEY (C_D_ID, C_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
     );
     CREATE INDEX IDX_CUSTOMER ON CUSTOMER (C_W_ID,C_D_ID,C_LAST);
     INDEXES:
     0, 1, 2 C_ID, C_W_ID, C_D_ID
     1, 2, 5 C_W_ID, C_D_ID, C_LAST
 */
/*
    CREATE TABLE HISTORY (
    0 H_C_ID INTEGER DEFAULT NULL,
    1 H_C_D_ID TINYINT DEFAULT NULL,
    2 H_C_W_ID SMALLINT DEFAULT NULL,
    3 H_D_ID TINYINT DEFAULT NULL,
    4 H_W_ID SMALLINT DEFAULT '0' NOT NULL,
    5 H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    6 H_AMOUNT FLOAT DEFAULT NULL,
    7 H_DATA VARCHAR(32) DEFAULT NULL,
    CONSTRAINT H_FKEY_C FOREIGN KEY (H_C_ID, H_C_D_ID, H_C_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID),
    CONSTRAINT H_FKEY_D FOREIGN KEY (H_D_ID, H_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
    );
 */
/*
   CREATE TABLE STOCK (
   0  S_I_ID INTEGER DEFAULT '0' NOT NULL REFERENCES ITEM (I_ID),
   1  S_W_ID SMALLINT DEFAULT '0 ' NOT NULL REFERENCES WAREHOUSE (W_ID),
   2  S_QUANTITY INTEGER DEFAULT '0' NOT NULL,
   3  S_DIST_01 VARCHAR(32) DEFAULT NULL,
   4  S_DIST_02 VARCHAR(32) DEFAULT NULL,
   5  S_DIST_03 VARCHAR(32) DEFAULT NULL,
   6  S_DIST_04 VARCHAR(32) DEFAULT NULL,
   7  S_DIST_05 VARCHAR(32) DEFAULT NULL,
   8  S_DIST_06 VARCHAR(32) DEFAULT NULL,
   9  S_DIST_07 VARCHAR(32) DEFAULT NULL,
   10 S_DIST_08 VARCHAR(32) DEFAULT NULL,
   11 S_DIST_09 VARCHAR(32) DEFAULT NULL,
   12 S_DIST_10 VARCHAR(32) DEFAULT NULL,
   13 S_YTD INTEGER DEFAULT NULL,
   14 S_ORDER_CNT INTEGER DEFAULT NULL,
   15 S_REMOTE_CNT INTEGER DEFAULT NULL,
   16 S_DATA VARCHAR(64) DEFAULT NULL,
   PRIMARY KEY (S_W_ID,S_I_ID)
   );
   INDEXES:
   0, 1 S_I_ID, S_W_ID
 */
/*
   CREATE TABLE ORDERS (
   0 O_ID INTEGER DEFAULT '0' NOT NULL,
   1 O_C_ID INTEGER DEFAULT NULL,
   2 O_D_ID TINYINT DEFAULT '0' NOT NULL,
   3 O_W_ID SMALLINT DEFAULT '0' NOT NULL,
   4 O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
   5 O_CARRIER_ID INTEGER DEFAULT NULL,
   6 O_OL_CNT INTEGER DEFAULT NULL,
   7 O_ALL_LOCAL INTEGER DEFAULT NULL,
   PRIMARY KEY (O_W_ID,O_D_ID,O_ID),
   UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID),
   CONSTRAINT O_FKEY_C FOREIGN KEY (O_C_ID, O_D_ID, O_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID)
   );
   CREATE INDEX IDX_ORDERS ON ORDERS (O_W_ID,O_D_ID,O_C_ID);
   INDEXES:
   0, 2, 3 O_ID, O_D_ID, O_W_ID
   1, 2, 3 O_C_ID, O_D_ID, O_W_ID
 */
/*
   CREATE TABLE NEW_ORDER (
   0 NO_O_ID INTEGER DEFAULT '0' NOT NULL,
   1 NO_D_ID TINYINT DEFAULT '0' NOT NULL,
   2 NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
   CONSTRAINT NO_PK_TREE PRIMARY KEY (NO_D_ID,NO_W_ID,NO_O_ID),
   CONSTRAINT NO_FKEY_O FOREIGN KEY (NO_O_ID, NO_D_ID, NO_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID)
   );
   INDEXES:
   0, 1, 2 NO_O_ID, NO_D_ID, NO_W_ID
 */
/*
   CREATE TABLE ORDER_LINE (
   0 OL_O_ID INTEGER DEFAULT '0' NOT NULL,
   1 OL_D_ID TINYINT DEFAULT '0' NOT NULL,
   2 OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
   3 OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
   4 OL_I_ID INTEGER DEFAULT NULL,
   5 OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
   6 OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
   7 OL_QUANTITY INTEGER DEFAULT NULL,
   8 OL_AMOUNT FLOAT DEFAULT NULL,
   9 OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
   PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER),
   CONSTRAINT OL_FKEY_O FOREIGN KEY (OL_O_ID, OL_D_ID, OL_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID),
   CONSTRAINT OL_FKEY_S FOREIGN KEY (OL_I_ID, OL_SUPPLY_W_ID) REFERENCES STOCK (S_I_ID, S_W_ID)
   );
   CREATE INDEX IDX_ORDER_LINE_TREE ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);
   INDEXES:
   0, 1, 2, 3 OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER
   0, 1, 2 OL_O_ID, OL_D_ID, OL_W_ID
 */

/////////////////////////////////////////////////////////
// TRANSACTIONS
/////////////////////////////////////////////////////////

std::vector<std::vector<Value>>
ExecuteReadTest(executor::AbstractExecutor* executor) {
  // Run all the executors
  bool status = executor->Init();
  if (status == false) {
    throw Exception("Init failed");
  }

  std::vector<std::vector<Value>> logical_tile_values;

  // Execute stuff
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());

    // is this possible?
    if(result_tile == nullptr)
      break;

    auto column_count = result_tile->GetColumnCount();

    for (oid_t tuple_id : *result_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(result_tile.get(),
                                                                  tuple_id);
      std::vector<Value> tuple_values;
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++){
        auto value = cur_tuple.GetValue(column_itr);
        tuple_values.push_back(value);
      }

      // Move the tuple list
      logical_tile_values.push_back(std::move(tuple_values));
    }
  }

  return std::move(logical_tile_values);
}

void ExecuteUpdateTest(executor::AbstractExecutor* executor) {
  // Run all the executors
  bool status = executor->Init();
  if (status == false) {
    throw Exception("Init failed");
  }

  // Execute stuff
  while (executor->Execute() == true);
}


}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton