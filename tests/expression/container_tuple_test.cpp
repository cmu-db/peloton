//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// container_tuple_test.cpp
//
// Identification: tests/expression/container_tuple_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>
#include <queue>

#include "harness.h"

#include "backend/common/types.h"
#include "backend/common/value_peeker.h"
#include "backend/common/value_factory.h"
#include "backend/expression/container_tuple.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace test {

class ContainerTupleTest : public PelotonTest {};

TEST_F(ContainerTupleTest, VectorValue) {
  std::vector<Value> values;
  values.push_back(ValueFactory::GetIntegerValue(11));
  values.push_back(ValueFactory::GetIntegerValue(22));
  values.push_back(ValueFactory::GetDoubleValue(3.14));
  values.push_back(
      ValueFactory::GetStringValue("Hello from ContainerTupleTest"));

  expression::ContainerTuple<std::vector<Value>> ctuple(&values);

  for (size_t i = 0; i < values.size(); i++) {
    LOG_INFO("%s",  ctuple.GetValue(i).GetInfo().c_str());
    EXPECT_TRUE(values[i].OpEquals(ctuple.GetValue(i)).IsTrue());
  }
}

}  // End test namespace
}  // End peloton namespace
