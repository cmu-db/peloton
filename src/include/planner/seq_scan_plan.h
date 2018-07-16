//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_plan.h
//
// Identification: src/include/planner/seq_scan_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/internal_types.h"
#include "common/logger.h"
#include "planner/abstract_scan_plan.h"
#include "type/serializer.h"

namespace peloton {
namespace planner {

class SeqScanPlan : public AbstractScan {
 public:
  // This constructor is needed for the deprecated PelotonService
  SeqScanPlan() : AbstractScan() {}

  // Normal plan
  SeqScanPlan(storage::DataTable *table,
              expression::AbstractExpression *predicate,
              const std::vector<oid_t> &column_ids, bool is_for_update = false,
              bool parallel = false)
      : AbstractScan(table, predicate, column_ids, parallel) {
    SetForUpdateFlag(is_for_update);
  }

  PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::SEQSCAN;
  }

  const std::string GetInfo() const override {
    return "SeqScanPlan(" + GetPredicateInfo() + ")";
  }

  void SetParameterValues(std::vector<type::Value> *values) override;

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  //===--------------------------------------------------------------------===//
  bool SerializeTo(SerializeOutput &output) const override;
  bool DeserializeFrom(SerializeInput &input) override;

  /* For init SerializeOutput */
  int SerializeSize() const override;

  std::unique_ptr<AbstractPlan> Copy() const override {
    auto *new_plan =
        new SeqScanPlan(GetTable(), GetPredicate()->Copy(), GetColumnIds());
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;
  bool operator!=(const AbstractPlan &rhs) const override {
    return !(*this == rhs);
  }

  void VisitParameters(
      codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

 private:
  // This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(SeqScanPlan);
};

}  // namespace planner
}  // namespace peloton
