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
    }
    else {

      std::vector<expression::TupleValueExpression *> column_exprs;
      for (size_t i = 0; i < columns_prop_ptr->GetSize(); ++i)
        column_exprs.push_back(columns_prop_ptr->GetColumn(i));

      // Add sort column to output property if needed
      auto sort_prop =
          requirements_.GetPropertyOfType(PropertyType::SORT)->As<PropertySort>();
      // column_exprs.empty() if
      // the sql is 'SELECT *'
      if (sort_prop != nullptr && !column_exprs.empty()) {
        for (size_t i = 0; i < sort_prop->GetSortColumnSize(); ++i) {
          auto sort_col = sort_prop->GetSortColumn(i);
          bool found = false;
          if (columns_prop_ptr != nullptr) {
            for (auto &col : column_exprs) {
              if (sort_col->bound_obj_id_ == col->bound_obj_id_) {
                found = true;
                break;
              }
            }
          }

          if (!found) column_exprs.push_back(sort_col);
        }
      }

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
void ChildPropertyGenerator::Visit(const PhysicalDelete *){
  // Let child fulfil all the required properties
  std::vector<PropertySet> child_input_properties{requirements_};

  output_.push_back(
      std::make_pair(requirements_, std::move(child_input_properties)));
};

} /* namespace optimizer */
} /* namespace peloton */
