//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_plan.h
//
// Identification: src/include/planner/seq_scan_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_scan_plan.h"
#include "common/logger.h"
#include "type/serializer.h"
#include "type/types.h"
#include "expression/abstract_expression.h"

namespace peloton {

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

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::SEQSCAN; }

  const std::string GetInfo() const { return "SeqScan"; }

  void SetParameterValues(std::vector<type::Value> *values);

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  //===--------------------------------------------------------------------===//
  bool SerializeTo(SerializeOutput &output) const ;
  bool DeserializeFrom(SerializeInput &input);

  /* For init SerializeOutput */
  int SerializeSize();

  oid_t GetColumnID(std::string col_name);

  std::unique_ptr<AbstractPlan> Copy() const {
    AbstractPlan *new_plan = new SeqScanPlan(
        this->GetTable(), this->GetPredicate()->Copy(), this->GetColumnIds());
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

 private:
  DISALLOW_COPY_AND_MOVE(SeqScanPlan);
};

}  // namespace planner
}  // namespace peloton