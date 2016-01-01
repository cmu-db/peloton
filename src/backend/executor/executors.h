//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// executors.h
//
// Identification: src/backend/executor/executors.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

// This is just for convenience

#include "backend/executor/aggregate_executor.h"
#include "backend/executor/limit_executor.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/nested_loop_join_executor.h"
#include "backend/executor/merge_join_executor.h"
#include "backend/executor/hash_join_executor.h"
#include "backend/executor/hash_executor.h"
#include "backend/executor/order_by_executor.h"
#include "backend/executor/hash_set_op_executor.h"
#include "backend/executor/append_executor.h"
#include "backend/executor/projection_executor.h"
