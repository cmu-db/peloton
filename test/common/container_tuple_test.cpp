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

class ContainerTupleTests : public PelotonTest {};

TEST_F(ContainerTupleTests, VectorValue) {

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(11));
  values.push_back(type::ValueFactory::GetIntegerValue(22));
  values.push_back(type::ValueFactory::GetDecimalValue(3.14));
  values.push_back(type::ValueFactory::GetVarcharValue("Hello from ContainerTupleTest"));

  ContainerTuple<std::vector<type::Value>> ctuple(&values);

  for (size_t i = 0; i < values.size(); i++) {
    LOG_INFO("%s", ctuple.GetValue(i).GetInfo().c_str());
    EXPECT_TRUE(values[i].CompareEquals(ctuple.GetValue(i)) == type::CMP_TRUE);
  }
}

}  // namespace test
}  // namespace peloton
