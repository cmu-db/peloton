//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_comparator.h
//
// Identification: src/include/codegen/plan_comparator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include <string>
#include <vector>
#include "planner/abstract_plan.h"
#include "planner/abstract_scan_plan.h"
#include "planner/aggregate_plan.h"
#include "planner/delete_plan.h"
#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "planner/insert_plan.h"
#include "planner/order_by_plan.h"
#include "planner/seq_scan_plan.h"
#include "planner/update_plan.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// ExpressionComparator provides compare function for all expressions that are
// currently supported by codegen.
//===----------------------------------------------------------------------===//
class ExpressionComparator {
 public:
  ExpressionComparator() = delete;
  static int Compare(const expression::AbstractExpression *,
                     const expression::AbstractExpression *);

 private:
  static int CompareChildren(const expression::AbstractExpression *,
                             const expression::AbstractExpression *);
};

//===----------------------------------------------------------------------===//
// PlanComparator provides compare function for all plans that are currently
// supported by codegen including SeqScanPlan, OrderByPlan, etc.
//===----------------------------------------------------------------------===//
class PlanComparator {
 public:
  PlanComparator() = delete;
  static int Compare(const planner::AbstractPlan &,
                     const planner::AbstractPlan &);

 private:
  // different compare functions for derived plan
  static int CompareInsert(const planner::InsertPlan &,
                           const planner::InsertPlan &);
  static int CompareDelete(const planner::DeletePlan &,
                           const planner::DeletePlan &);
  //  static int CompareInsert(const planner::InsertPlan &, const
  //  planner::InsertPlan &);

  static int CompareSeqScan(const planner::SeqScanPlan &,
                            const planner::SeqScanPlan &);
  static int CompareOrderBy(const planner::OrderByPlan &,
                            const planner::OrderByPlan &);
  static int CompareAggregate(const planner::AggregatePlan &,
                              const planner::AggregatePlan &);
  static int CompareHash(const planner::HashPlan &, const planner::HashPlan &);
  static int CompareHashJoin(const planner::HashJoinPlan &,
                             const planner::HashJoinPlan &);

  // Helper compare functions
  static int CompareProjectInfo(const planner::ProjectInfo *,
                                const planner::ProjectInfo *);
  static int CompareDerivedAttr(const planner::DerivedAttribute &,
                                const planner::DerivedAttribute &);
  static int CompareAggType(
      const std::vector<planner::AggregatePlan::AggTerm> &,
      const std::vector<planner::AggregatePlan::AggTerm> &);
  static int CompareSchema(const catalog::Schema &, const catalog::Schema &);
  static int CompareChildren(const planner::AbstractPlan &,
                             const planner::AbstractPlan &);
};
}
}