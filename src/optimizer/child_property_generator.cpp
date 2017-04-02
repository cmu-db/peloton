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

#include "expression/expression_util.h"
#include "optimizer/child_property_generator.h"
#include "optimizer/column_manager.h"
#include "optimizer/properties.h"

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

  auto columns_prop = requirements_.GetPropertyOfType(PropertyType::COLUMNS);
  auto predicate_prop =
      requirements_.GetPropertyOfType(PropertyType::PREDICATE);

  if (predicate_prop != nullptr) {
    provided_property.AddProperty(predicate_prop);
  }

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
      
      // Insert all missing TupleValueExpressions in the Predicate
      auto predicate_prop = requirements_.GetPropertyOfType(PropertyType::PREDICATE)
                           ->As<PropertyPredicate>();
      if (predicate_prop != nullptr) {
        expression::ExpressionUtil::GetTupleValueExprs(column_set,
             predicate_prop->GetPredicate());
      }
      
      
      // Insert all missing TupleValueExpressions in Sort expressions
      auto sort_prop = requirements_.GetPropertyOfType(PropertyType::SORT)
                           ->As<PropertySort>();
      if (sort_prop != nullptr) {
        for (size_t i = 0; i < sort_prop->GetSortColumnSize(); ++i) {
          expression::ExpressionUtil::GetTupleValueExprs(column_set,
                                                         sort_prop->GetSortColumn(i));
        }
      }
      
      // Insert all missing TupleValueExpressions in Projection expressions
      auto proj_prop = requirements_.GetPropertyOfType(PropertyType::PROJECT)
              ->As<PropertyProjection>();
      if (proj_prop != nullptr) {
        for (size_t i = 0; i < proj_prop->GetProjectionListSize(); i++) {
          expression::ExpressionUtil::GetTupleValueExprs(column_set,
                                                         proj_prop->GetProjection(i));
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
