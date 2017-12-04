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

#include "optimizer/child_property_deriver.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "optimizer/memo.h"

using std::move;
using std::vector;
using std::make_pair;
using std::vector;
using std::shared_ptr;
using std::pair;
using std::make_shared;

namespace peloton {
namespace optimizer {

vector<pair<shared_ptr<PropertySet>, vector<shared_ptr<PropertySet>>>>
ChildPropertyDeriver::GetProperties(GroupExpression *gexpr,
                                    PropertySet *requirements, Memo *memo) {
  requirements_ = requirements;
  output_.clear();
  memo_ = memo;
  gexpr->Op().Accept(this);
  return move(output_);
}

void ChildPropertyDeriver::Visit(const PhysicalSeqScan *) {
  // Seq Scan does not provide any property
  output_.push_back(
      make_pair(make_shared<PropertySet>(), vector<shared_ptr<PropertySet>>{}));
};

void ChildPropertyDeriver::Visit(const PhysicalIndexScan *) {
  // TODO(boweic): Index Scan should provide sort property if the index is built
  // on sort column
  output_.push_back(
      make_pair(make_shared<PropertySet>(), vector<shared_ptr<PropertySet>>{}));
}

void ChildPropertyDeriver::Visit(const QueryDerivedScan *) {
  // TODO(boweic): QueryDerivedScan should be deprecated when we added schema
  // for logical groups
  output_.push_back(make_pair(requirements_, vector<PropertySet>{requirements_});
}

/**
 * Note:
 * Fulfill the entire projection property in the aggregation. Should
 * enumerate different combination of the aggregation functions and other
 * projection.
 */
void ChildPropertyDeriver::Visit(const PhysicalHashGroupBy *) {
  output_.push_back(
      make_pair(make_shared<PropertySet>(),
                vector<shared_ptr<PropertySet>>{make_shared<PropertySet>()}));
}

void ChildPropertyDeriver::Visit(const PhysicalSortGroupBy *) {
  // TODO(boweic): add sort requirement
  output_.push_back(make_pair(requirements_, vector<PropertySet>{requirements_});
}

void ChildPropertyDeriver::Visit(const PhysicalAggregate *) {
  // output_.push_back(make_pair(requirements_,
  // vector<PropertySet>{requirements_});
  output_.push_back(
      make_pair(make_shared<PropertySet>(),
                vector<shared_ptr<PropertySet>>{make_shared<PropertySet>()}));
}

void ChildPropertyDeriver::Visit(const PhysicalLimit *) {}
void ChildPropertyDeriver::Visit(const PhysicalDistinct *) {}
void ChildPropertyDeriver::Visit(const PhysicalProject *) {}
void ChildPropertyDeriver::Visit(const PhysicalOrderBy *) {}
void ChildPropertyDeriver::Visit(const PhysicalFilter *) {}
void ChildPropertyDeriver::Visit(const PhysicalInnerNLJoin *) {
  // TODO(boweic): Check if sort could be pass down
  output_.push_back(make_pair(
      make_shared<PropertySet>(),
      vector<shared_ptr<PropertySet>>(2, make_shared<PropertySet>())));
}
void ChildPropertyDeriver::Visit(const PhysicalLeftNLJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalRightNLJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalOuterNLJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalInnerHashJoin *) {
  // TODO(boweic): Check if sort could be pass down
  output_.push_back(make_pair(
      make_shared<PropertySet>(),
      vector<shared_ptr<PropertySet>>(2, make_shared<PropertySet>())));
}

void ChildPropertyDeriver::Visit(const PhysicalLeftHashJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalRightHashJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalOuterHashJoin *) {}
void ChildPropertyDeriver::Visit(const PhysicalInsert *) {}
void ChildPropertyDeriver::Visit(const PhysicalInsertSelect *) {
  // Let child fulfil all the required properties
  vector<PropertySet> child_input_properties{requirements_};

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
}
void ChildPropertyDeriver::Visit(const PhysicalUpdate *) {
  // Let child fulfil all the required properties
  vector<PropertySet> child_input_properties{requirements_};

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
}
void ChildPropertyDeriver::Visit(const PhysicalDelete *) {
  // Let child fulfil all the required properties
  vector<PropertySet> child_input_properties{requirements_};

  output_.push_back(make_pair(requirements_, move(child_input_properties)));
};

void ChildPropertyDeriver::Visit(const DummyScan *) {
  // Provide nothing
  output_.push_back(make_pair(PropertySet(), vector<PropertySet>()));
}

}  // namespace optimizer
}  // namespace peloton
