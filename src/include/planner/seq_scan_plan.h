//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_plan.h
//
// Identification: src/include/planner/seq_scan_plan.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_scan_plan.h"
#include "common/logger.h"
#include "expression/abstract_expression.h"
#include "type/serializer.h"
#include "common/internal_types.h"

namespace peloton {

namespace expression {
class Parameter;
}
namespace parser {
class SelectStatement;
}
namespace storage {
class DataTable;
}

namespace planner {

class SeqScanPlan : public AbstractScan {
 public:
  SeqScanPlan(storage::DataTable *table,
              expression::AbstractExpression *predicate,
              const std::vector<oid_t> &column_ids, bool is_for_update = false)
      : AbstractScan(table, predicate, column_ids) {
    LOG_TRACE("Creating a Sequential Scan Plan");

    SetForUpdateFlag(is_for_update);
  }

  SeqScanPlan() : AbstractScan() {}

  PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::SEQSCAN;
  }

  const std::string GetInfo() const override { return "SeqScan"; }

  void SetParameterValues(std::vector<type::Value> *values) override;

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  //===--------------------------------------------------------------------===//
  bool SerializeTo(SerializeOutput &output) const override;
  bool DeserializeFrom(SerializeInput &input) override;

  /* For init SerializeOutput */
  int SerializeSize() const override;

  oid_t GetColumnID(std::string col_name);

  std::unique_ptr<AbstractPlan> Copy() const override {
    AbstractPlan *new_plan = new SeqScanPlan(
        this->GetTable(), this->GetPredicate()->Copy(), this->GetColumnIds());
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;
  bool operator!=(const AbstractPlan &rhs) const override {
    return !(*this == rhs);
  }

  virtual void VisitParameters(codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user) override;

 private:
  DISALLOW_COPY_AND_MOVE(SeqScanPlan);
};

}  // namespace planner
}  // namespace peloton
