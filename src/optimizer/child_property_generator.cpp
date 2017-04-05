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

using std::move;
using std::vector;
using std::make_pair;
using std::vector;
using std::shared_ptr;
using std::pair;

namespace peloton {
namespace optimizer {

vector<pair<PropertySet, vector<PropertySet>>>
ChildPropertyGenerator::GetProperties(shared_ptr<GroupExpression> gexpr,
                                      PropertySet requirements) {
  requirements_ = requirements;
  output_.clear();

  gexpr->Op().Accept(this);

  if (output_.empty()) {
    output_.push_back(make_pair(requirements_, vector<PropertySet>()));
  }

  return move(output_);
}

void ChildPropertyGenerator::Visit(const PhysicalLimit *) {
  // Let child fulfil all the required properties
  vector<PropertySet> child_input_properties{requirements_};
  output_.push_back(make_pair(requirements_, move(child_input_properties)));
}

void ChildPropertyGenerator::Visit(const PhysicalScan *) {
  PropertySet provided_property;

  // Scan will fullfill PropertyPredicate and PropertyColumns
  auto predicate_prop =
      requirements_.GetPropertyOfType(PropertyType::PREDICATE);
  if (predicate_prop != nullptr)
    provided_property.AddProperty(predicate_prop);

  auto columns_prop = requirements_.GetPropertyOfType(PropertyType::COLUMNS);
  if (columns_prop != nullptr)
    provided_property.AddProperty(columns_prop);

  output_.push_back(make_pair(move(provided_property), vector<PropertySet>()));
};

/**
 * Note:
 * 1. Assume that the aggregation will break the sort property for now.
 *    Should check the agg_type and see whether sort property can be fulfilled.
 * 2. Fulfill the entire projection property in the aggregation. Should
 * enumerate
 *    different combination of the aggregation functions and other projection.
 */

void ChildPropertyGenerator::Visit(const PhysicalAggregate *) {
  PropertySet child_input_property_set;
  PropertySet provided_property;

  for (auto prop : requirements_.Properties()) {
    switch (prop->Type()) {
      // Generate output columns for the child
      // Aggregation will break sort property
      case PropertyType::SORT:
        break;
      case PropertyType::COLUMNS: {
        // PropertyColumns will be fulfilled by the child operator
        child_input_property_set.AddProperty(prop);
        provided_property.AddProperty(prop);
        break;
      }
      case PropertyType::PREDICATE:
        // PropertyPredicate will be fulfilled by the child operator
        child_input_property_set.AddProperty(prop);
        provided_property.AddProperty(prop);
        break;
      case PropertyType::PROJECT:
        // PropertyProject will be fulfilled by this operator
        provided_property.AddProperty(prop);
        break;
    }
  }
  vector<PropertySet> child_input_properties{child_input_property_set};
  output_.push_back(make_pair(provided_property, move(child_input_properties)));
}

void ChildPropertyGenerator::Visit(const PhysicalHash *) {
  PropertySet child_input_property_set;
  PropertySet provided_property;

  for (auto prop : requirements_.Properties()) {
    switch (prop->Type()) {
      // Hash will break sort property
      case PropertyType::SORT: {
        break;
      }
      case PropertyType::COLUMNS: {
        // PropertyColumns will be fulfilled by the child operator
        child_input_property_set.AddProperty(prop);
        provided_property.AddProperty(prop);
        break;
      }
      case PropertyType::PREDICATE: {
        // PropertyPredicate will be fulfilled by the child operator
        child_input_property_set.AddProperty(prop);
        provided_property.AddProperty(prop);
        break;
      }
      case PropertyType::PROJECT: {
        // PropertyProject will be fulfilled by the child operator
        provided_property.AddProperty(prop);
        break;
      }
    }
  }

  vector<PropertySet> child_input_properties{child_input_property_set};
  output_.push_back(make_pair(provided_property, move(child_input_properties)));

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
  vector<PropertySet> child_input_properties{requirements_};

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
};

} /* namespace optimizer */
} /* namespace peloton */
