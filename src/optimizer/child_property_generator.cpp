//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// child_property_generator.cpp
//
// Identification: src/include/optimizer/child_property_generator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/child_property_generator.h"
#include "optimizer/column_manager.h"
#include "optimizer/properties.h"
#include "expression/expression_util.h"

namespace peloton {
namespace optimizer {

std::vector<std::pair<PropertySet, std::vector<PropertySet>>>
ChildPropertyGenerator::GetProperties(std::shared_ptr<GroupExpression> gexpr,
                                      PropertySet requirements) {
  requirements_ = requirements;
  output_.clear();

  gexpr->Op().Accept(this);

  if (output_.empty()) {
    output_.push_back(
        std::make_pair(requirements_, std::vector<PropertySet>()));
  }

  return std::move(output_);
}

void ChildPropertyGenerator::Visit(const PhysicalLimit *) {
  // Let child fulfil all the required properties
  std::vector<PropertySet> child_input_properties{requirements_};
  output_.push_back(
      std::make_pair(requirements_, std::move(child_input_properties)));
}

void ChildPropertyGenerator::Visit(const PhysicalScan *) {
  PropertySet provided_property;
  
  // Scan will fullfill predicate property
  auto predicate_prop =
      requirements_.GetPropertyOfType(PropertyType::PREDICATE);
  if (predicate_prop != nullptr) {
    provided_property.AddProperty(predicate_prop);
  }
  
  auto columns_prop = requirements_.GetPropertyOfType(PropertyType::COLUMNS);
  if (columns_prop != nullptr) {
    auto columns_prop_ptr = columns_prop->As<PropertyColumns>();
    if (columns_prop_ptr->IsStarExpressionInColumn()) {
      provided_property.AddProperty(columns_prop);
    } else {
      // This set has all the TupleValueExpr that appears separately in the
      // Statement. For example, SELECT a, b, b + c FROM A ORDER BY c + d
      // TupleValueExpr a, b will be in the PropertyColumn. However, there are
      // other expressions like b + c or c + d that need scan to also return
      // columns c and d. Thus, here we add all the missing columns that appears
      // in the expression but not in PropertyColumn.
      ExprSet column_set;
      for (size_t i = 0; i < columns_prop_ptr->GetSize(); ++i)
        column_set.insert(columns_prop_ptr->GetColumn(i));

      // Insert all missing TupleValueExpressions in Sort expressions
      auto sort_prop = requirements_.GetPropertyOfType(PropertyType::SORT)
                           ->As<PropertySort>();
      if (sort_prop != nullptr) {
        for (size_t i = 0; i < sort_prop->GetSortColumnSize(); ++i) {
          expression::ExpressionUtil::GetTupleValueExprs(
              column_set, sort_prop->GetSortColumn(i));
        }
      }

      // Insert all missing TupleValueExpressions in Projection expressions
      auto proj_prop = requirements_.GetPropertyOfType(PropertyType::PROJECT)
                           ->As<PropertyProjection>();
      if (proj_prop != nullptr) {
        for (size_t i = 0; i < proj_prop->GetProjectionListSize(); i++) {
          expression::ExpressionUtil::GetTupleValueExprs(
              column_set, proj_prop->GetProjection(i));
        }
      }

      std::vector<expression::AbstractExpression *> column_exprs(
          column_set.begin(), column_set.end());
      provided_property.AddProperty(
          std::shared_ptr<PropertyColumns>(new PropertyColumns(column_exprs)));
    }
  }
  output_.push_back(
      std::make_pair(std::move(provided_property), std::vector<PropertySet>()));
};

/**
 * Note:
 * 1. Assume that the aggregation will break the sort property for now.
 *    Should check the agg_type and see whether sort property can be fulfilled.
 * 2. Fulfill the entire projection property in the aggregation. Should enumerate
 *    different combination of the aggregation functions and other projection.
 */

void ChildPropertyGenerator::Visit(const PhysicalAggregate *op) {
  PropertySet child_input_property_set;
  PropertySet provided_property;
  ExprSet child_cols;
  bool has_star_expr = false;

  for (auto prop : requirements_.Properties()) {
    switch(prop->Type()){
      // Generate output columns for the child
      // Aggregation will break sort property
      case PropertyType::SORT:break;
      case PropertyType::COLUMNS: {
        provided_property.AddProperty(prop);
        auto col_prop = prop->As<PropertyColumns>();
        has_star_expr = col_prop->IsStarExpressionInColumn();
        if (has_star_expr) break;

        //TODO: Store ExprSet in property columns
        // Reuqire group by columns in the child node
        size_t col_size = col_prop->GetSize();
        for (size_t i = 0; i < col_size; i++)
          child_cols.insert(col_prop->GetColumn(i));
        for (auto group_by_col : *op->columns)
          child_cols.insert(group_by_col);

        break;
      }
      case PropertyType::PREDICATE:
        child_input_property_set.AddProperty(prop);
        provided_property.AddProperty(prop);
        break;
      case PropertyType::PROJECT:
        provided_property.AddProperty(prop);
        auto proj_prop = prop->As<PropertyProjection>();
        for (size_t idx = 0; idx < proj_prop->GetProjectionListSize(); idx++) {
          auto expr = proj_prop->GetProjection(idx);
          expression::ExpressionUtil::GetTupleValueExprs(child_cols, expr);
        }
        break;
    }
  }
  child_input_property_set.AddProperty(
      std::make_shared<PropertyColumns>(
          std::vector<expression::AbstractExpression*>(
              child_cols.begin(), child_cols.end()),
          has_star_expr));
  std::vector<PropertySet> child_input_properties{child_input_property_set};

  output_.push_back(std::make_pair(provided_property, std::move(child_input_properties)));
}

void ChildPropertyGenerator::Visit(const PhysicalProject *){};
void ChildPropertyGenerator::Visit(const PhysicalOrderBy *) {}
void ChildPropertyGenerator::Visit(const PhysicalFilter *){};
void ChildPropertyGenerator::Visit(const PhysicalInnerNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalLeftNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalRightNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalOuterNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalInnerHashJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalLeftHashJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalRightHashJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalOuterHashJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalInsert *){};
void ChildPropertyGenerator::Visit(const PhysicalUpdate *){};
void ChildPropertyGenerator::Visit(const PhysicalDelete *) {
  // Let child fulfil all the required properties
  std::vector<PropertySet> child_input_properties{requirements_};

  output_.push_back(
      std::make_pair(requirements_, std::move(child_input_properties)));
};

} /* namespace optimizer */
} /* namespace peloton */
