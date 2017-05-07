//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_and_stats_calculator.h
//
// Identification: src/include/optimizer/cost_and_stats_calculator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/cost_and_stats_calculator.h"
#include "optimizer/column_manager.h"
#include "optimizer/stats.h"
#include "optimizer/properties.h"

namespace peloton {
namespace optimizer {

void CostAndStatsCalculator::CalculateCostAndStats(
    std::shared_ptr<GroupExpression> gexpr,
    const PropertySet *output_properties,
    const std::vector<PropertySet> *input_properties_list,
    std::vector<std::shared_ptr<Stats>> child_stats,
    std::vector<double> child_costs) {
  gexpr_ = gexpr;
  output_properties_ = output_properties;
  input_properties_list_ = input_properties_list;
  child_stats_ = child_stats;
  child_costs_ = child_costs;

  gexpr->Op().Accept(this);
}

void CostAndStatsCalculator::Visit(const DummyScan *) {
  // TODO: Replace with more accurate cost
  output_stats_.reset(new Stats(nullptr));
  output_cost_ = 0;
}

void CostAndStatsCalculator::Visit(const PhysicalSeqScan *) {
  // TODO : Replace with more accurate cost
  output_stats_.reset(new Stats(nullptr));
  output_cost_ = 1;
};
void CostAndStatsCalculator::Visit(const PhysicalIndexScan *op) {
  // Simple cost function
  // indexSearchable ? Index : SeqScan
  // TODO : Replace with more accurate cost
  output_stats_.reset(new Stats(nullptr));
  auto predicate_prop =
      output_properties_->GetPropertyOfType(PropertyType::PREDICATE)
          ->As<PropertyPredicate>();

  if (predicate_prop == nullptr) {
    output_cost_ = 2;
    return;
  }

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  oid_t index_id = 0;

  expression::AbstractExpression *predicate = predicate_prop->GetPredicate();
  if (util::CheckIndexSearchable(op->table_, predicate, key_column_ids,
                                 expr_types, values, index_id)) {
    output_cost_ = 0;
  } else {
    output_cost_ = 2;
  }
};
void CostAndStatsCalculator::Visit(const PhysicalProject *) {
  // TODO: Replace with more accurate cost
  output_stats_.reset(new Stats(nullptr));
  output_cost_ = 0;
}
void CostAndStatsCalculator::Visit(const PhysicalOrderBy *) {
  // TODO: Replace with more accurate cost
  output_cost_ = 0;
}
void CostAndStatsCalculator::Visit(const PhysicalLimit *) {
  // TODO: Replace with more accurate cost
  output_cost_ = 0;
}
void CostAndStatsCalculator::Visit(const PhysicalFilter *){};
void CostAndStatsCalculator::Visit(const PhysicalInnerNLJoin *){
  // TODO: Replace with more accurate cost
  output_cost_ = 1;
};
void CostAndStatsCalculator::Visit(const PhysicalLeftNLJoin *){};
void CostAndStatsCalculator::Visit(const PhysicalRightNLJoin *){};
void CostAndStatsCalculator::Visit(const PhysicalOuterNLJoin *){};
void CostAndStatsCalculator::Visit(const PhysicalInnerHashJoin *){
  // TODO: Replace with more accurate cost
  output_cost_ = 0;
};
void CostAndStatsCalculator::Visit(const PhysicalLeftHashJoin *){};
void CostAndStatsCalculator::Visit(const PhysicalRightHashJoin *){};
void CostAndStatsCalculator::Visit(const PhysicalOuterHashJoin *){};
void CostAndStatsCalculator::Visit(const PhysicalInsert *) {
  // TODO: Replace with more accurate cost
  output_cost_ = 0;
};
void CostAndStatsCalculator::Visit(const PhysicalDelete *) {
  // TODO: Replace with more accurate cost
  output_cost_ = 0;
};
void CostAndStatsCalculator::Visit(const PhysicalUpdate *) {
  // TODO: Replace with more accurate cost
  output_cost_ = 0;
};
void CostAndStatsCalculator::Visit(const PhysicalHashGroupBy *) {
  // TODO: Replace with more accurate cost
  output_cost_ = 0;
};
void CostAndStatsCalculator::Visit(const PhysicalSortGroupBy *) {
  // TODO: Replace with more accurate cost
  output_cost_ = 0;
};
void CostAndStatsCalculator::Visit(const PhysicalAggregate *) {
  // TODO: Replace with more accurate cost
  output_cost_ = 0;
};
void CostAndStatsCalculator::Visit(const PhysicalDistinct *) {
  // TODO: Replace with more accurate cost
  output_cost_ = 0;
};

} /* namespace optimizer */
} /* namespace peloton */
