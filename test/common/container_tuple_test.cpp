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

#include "type/value_peeker.h"
#include "common/container_tuple.h"
#include "storage/table_factory.h"

namespace peloton {
namespace test {

class ContainerTupleTests : public PelotonTest {};

TEST_F(ContainerTupleTests, VectorValue) {
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(11));
  values.push_back(type::ValueFactory::GetIntegerValue(22));
  values.push_back(type::ValueFactory::GetDecimalValue(3.14));
  values.push_back(
      type::ValueFactory::GetVarcharValue("Hello from ContainerTupleTest"));

  ContainerTuple<std::vector<type::Value>> ctuple(&values);

  for (size_t i = 0; i < values.size(); i++) {
    LOG_INFO("%s", ctuple.GetValue(i).GetInfo().c_str());
    EXPECT_TRUE(values[i].CompareEquals(ctuple.GetValue(i)) == CmpBool::TRUE);
  }
}

TEST_F(ContainerTupleTests, GetInfo) {
  catalog::Column a_col{type::TypeId::INTEGER,
                        type::Type::GetTypeSize(type::TypeId::INTEGER), "a"};
  catalog::Column b_col{type::TypeId::BIGINT,
                        type::Type::GetTypeSize(type::TypeId::BIGINT), "b"};
  catalog::Column c_col{type::TypeId::VARCHAR, 50, "c"};

  catalog::Schema test_schema{{a_col, b_col, c_col}};

  std::unique_ptr<storage::AbstractTable> temp_table{
      storage::TableFactory::GetTempTable(&test_schema, false)};

  auto *pool = TestingHarness::GetInstance().GetTestingPool();

  storage::Tuple tuple1{&test_schema, true};
  tuple1.SetValue(0, type::ValueFactory::GetIntegerValue(1), pool);
  tuple1.SetValue(1, type::ValueFactory::GetBigIntValue(2), pool);
  tuple1.SetValue(2, type::ValueFactory::GetVarcharValue("Hello"), pool);

  auto pos = temp_table->InsertTuple(&tuple1);

  auto tile_group = temp_table->GetTileGroupById(pos.block);
  auto tuple_id = pos.offset;

  // Now test
  {
    // Check all columns
    peloton::ContainerTuple<storage::TileGroup> test_tuple{tile_group.get(),
                                                           tuple_id};
    EXPECT_EQ(test_tuple.GetInfo(), "(INTEGER(1),BIGINT(2),VARCHAR[6](Hello))");
  }

  {
    // Check subset with integer and varchar
    std::vector<oid_t> subset = {0, 2};
    peloton::ContainerTuple<storage::TileGroup> test_tuple{tile_group.get(),
                                                           tuple_id, &subset};
    EXPECT_EQ(test_tuple.GetInfo(), "(INTEGER(1),VARCHAR[6](Hello))");
  }

  {
    // Check re-arranged subset
    std::vector<oid_t> subset = {2, 0};
    peloton::ContainerTuple<storage::TileGroup> test_tuple{tile_group.get(),
                                                           tuple_id, &subset};
    EXPECT_EQ(test_tuple.GetInfo(), "(VARCHAR[6](Hello),INTEGER(1))");
  }
}

}  // namespace test
}  // namespace peloton
