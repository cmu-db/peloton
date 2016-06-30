
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hybrid_scan_plan.cpp
//
// Identification: /peloton/src/planner/hybrid_scan_plan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/hybrid_scan_plan.h"
#include "planner/index_scan_plan.h"
#include "storage/data_table.h"
#include "index/index.h"
#include "common/types.h"
#include "expression/abstract_expression.h"

namespace peloton {
namespace planner {
	HybridScanPlan::HybridScanPlan(storage::DataTable *table,
               expression::AbstractExpression *predicate,
               const std::vector<oid_t> &column_ids,
               const IndexScanPlan::IndexScanDesc &index_scan_desc,
               HybridScanType hybrid_scan_type)
    : AbstractScan(table, predicate, column_ids),
      type_(hybrid_scan_type),
      column_ids_(column_ids),
      key_column_ids_(std::move(index_scan_desc.key_column_ids)),
      expr_types_(std::move(index_scan_desc.expr_types)),
      values_(std::move(index_scan_desc.values)),
      runtime_keys_(std::move(index_scan_desc.runtime_keys)),
      index_(index_scan_desc.index){}

}
}
