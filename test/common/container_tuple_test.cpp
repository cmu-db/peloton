//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// container_tuple_test.cpp
//
// Identification: test/common/container_tuple_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iostream>
#include <sstream>
#include <queue>

#include "common/harness.h"

#include "type/types.h"
#include "type/value_peeker.h"
#include "type/value_factory.h"
#include "common/container_tuple.h"
#include "storage/tuple.h"

namespace peloton {
namespace test {

class ContainerTupleTest : public PelotonTest {};
/*
TEST_F(ContainerTupleTest, VectorValue) {
  std::vector<common::Value &> values;
  values.push_back(common::ValueFactory::GetIntegerValue(11));
  values.push_back(common::ValueFactory::GetIntegerValue(22));
  values.push_back(common::ValueFactory::GetDoubleValue(3.14));
  values.push_back(
      common::ValueFactory::GetVarcharValue("Hello from ContainerTupleTest"));

  expression::ContainerTuple<std::vector<common::Value *>> ctuple(&values);

  for (size_t i = 0; i < values.size(); i++) {
    LOG_INFO("%s", ctuple.GetValue(i)->GetInfo().c_str());
    EXPECT_TRUE(values[i].CompareEquals(ctuple.GetValue(i))->IsTrue());
  }
}
*/
}  // End test namespace
}  // End peloton namespace
