//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executors.h
//
// Identification: src/include/executor/executors.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

// This is just for convenience

#include "create_executor.h"
#include "drop_executor.h"
#include "executor/aggregate_executor.h"
#include "executor/limit_executor.h"
#include "executor/materialization_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/index_scan_executor.h"
#include "executor/insert_executor.h"
#include "executor/delete_executor.h"
#include "executor/update_executor.h"
#include "executor/nested_loop_join_executor.h"
#include "executor/merge_join_executor.h"
#include "executor/hash_join_executor.h"
#include "executor/hash_executor.h"
#include "executor/order_by_executor.h"
#include "executor/hash_set_op_executor.h"
#include "executor/append_executor.h"
#include "executor/projection_executor.h"
#include "executor/copy_executor.h"
