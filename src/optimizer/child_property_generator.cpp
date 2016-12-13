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

namespace peloton {
namespace optimizer {

std::vector<std::pair<PropertySet, std::vector<PropertySet>>>
ChildPropertyGenerator::GetProperties(std::shared_ptr<GroupExpression> gexpr,
                                      PropertySet requirements) {
  requirements_ = requirements;
  output_.clear();

  gexpr->Op().Accept(this);

  return std::move(output_);
}

void ChildPropertyGenerator::Visit(const PhysicalScan *) {
  output_.push_back(std::make_pair(requirements_, std::vector<PropertySet>()));
};

void ChildPropertyGenerator::Visit(const PhysicalProject *) {}
void ChildPropertyGenerator::Visit(const PhysicalComputeExprs *){};
void ChildPropertyGenerator::Visit(const PhysicalFilter *){};
void ChildPropertyGenerator::Visit(const PhysicalInnerNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalLeftNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalRightNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalOuterNLJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalInnerHashJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalLeftHashJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalRightHashJoin *){};
void ChildPropertyGenerator::Visit(const PhysicalOuterHashJoin *){};

} /* namespace optimizer */
} /* namespace peloton */
