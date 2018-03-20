//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// masked_tuple_test.cpp
//
// Identification: test/storage/masked_tuple_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include <vector>

#include "storage/masked_tuple.h"
#include "storage/tuple.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

std::unique_ptr<catalog::Schema> key_schema(nullptr);
std::unique_ptr<catalog::Schema> tuple_schema(nullptr);

const int NUM_COLUMNS = 5;

//===--------------------------------------------------------------------===//
// MaskedTuple Tests
//===--------------------------------------------------------------------===//

class MaskedTupleTests : public PelotonTest {};

void MaskedTupleTestHelper(storage::MaskedTuple *masked_tuple,
                           std::vector<int> values, std::vector<oid_t> mask) {
  for (int i = 0; i < NUM_COLUMNS; i++) {
    auto v = masked_tuple->GetValue(i);
    int expected = values[mask[i]];
    auto result =
        v.CompareEquals(type::ValueFactory::GetIntegerValue(expected));
    LOG_TRACE("mask[%d]->%d ==> (Expected=%d / Result=%s)", i, mask[i],
              expected, v.GetInfo().c_str());
    EXPECT_EQ(CmpBool::TRUE, result);
  }
}

TEST_F(MaskedTupleTests, BasicTest) {
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  // Build tuple and key schema
  std::vector<catalog::Column> column_list;
  std::vector<oid_t> key_attrs;

  const char column_char = 'A';
  for (int i = 0; i < NUM_COLUMNS; i++) {
    std::ostringstream os;
    os << static_cast<char>((int)column_char + i);
    catalog::Column column(type::TypeId::INTEGER,
                           type::Type::GetTypeSize(type::TypeId::INTEGER),
                           os.str(), true);
    column_list.push_back(column);
    key_attrs.push_back(i);
  }  // FOR
  key_schema.reset(new catalog::Schema(column_list));
  key_schema->SetIndexedColumns(key_attrs);
  tuple_schema.reset(new catalog::Schema(column_list));

  // CREATE REAL TUPLE
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(tuple_schema.get(),
                                                           true));
  std::vector<int> values;
  for (int i = 0; i < NUM_COLUMNS; i++) {
    int val = (10 * i) + i;
    tuple->SetValue(i, type::ValueFactory::GetIntegerValue(val), pool);
    values.push_back(val);
  }  // FOR
  for (int i = 0; i < NUM_COLUMNS; i++) {
    auto v = tuple->GetValue(i);
    int expected = values[i];
    auto result =
        v.CompareEquals(type::ValueFactory::GetIntegerValue(expected));
    EXPECT_EQ(CmpBool::TRUE, result);
  }

  // CREATE MASKED TUPLE
  std::vector<oid_t> mask;
  for (int i = NUM_COLUMNS - 1; i >= 0; i--) {
    mask.push_back(i);
  }
  storage::MaskedTuple masked_tuple(tuple.get(), mask);
  MaskedTupleTestHelper(&masked_tuple, values, mask);
  EXPECT_FALSE(tuple->EqualsNoSchemaCheck(masked_tuple, mask));

  // SHOW THAT WE CAN REUSE MASKED TUPLE
  std::vector<oid_t> new_mask;
  for (int i = 0; i < NUM_COLUMNS; i++) {
    new_mask.push_back(1);
  }
  masked_tuple.SetMask(new_mask);
  MaskedTupleTestHelper(&masked_tuple, values, new_mask);
  EXPECT_TRUE(tuple->EqualsNoSchemaCheck(masked_tuple, new_mask));
}

}  // namespace test
}  // namespace peloton
