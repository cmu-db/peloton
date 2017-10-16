
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type_util_test.cpp
//
// Identification: /peloton/test/type/type_util_test.cpp
//
// Copyright (c) 2015-17 Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "storage/tuple.h"
#include "type/type_util.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// TypeUtil Tests
//===--------------------------------------------------------------------===//

class TypeUtilTests : public PelotonTest {};

catalog::Schema* TypeUtilTestsGenerateSchema() {
  // Construct Tuple Schema
  std::vector<type::TypeId> col_types = {
      type::TypeId::BOOLEAN,   type::TypeId::TINYINT, type::TypeId::SMALLINT,
      type::TypeId::INTEGER,   type::TypeId::BIGINT,  type::TypeId::DECIMAL,
      type::TypeId::TIMESTAMP, type::TypeId::VARCHAR};
  std::vector<catalog::Column> column_list;
  const int num_cols = (int)col_types.size();
  const char column_char = 'A';
  for (int i = 0; i < num_cols; i++) {
    std::ostringstream os;
    os << static_cast<char>((int)column_char + i);

    bool inlined = true;
    int length = type::Type::GetTypeSize(col_types[i]);
    if (col_types[i] == type::TypeId::VARCHAR) {
      inlined = false;
      length = 32;
    }

    catalog::Column column(col_types[i], length, os.str(), inlined);
    column_list.push_back(column);
  }
  catalog::Schema* schema = new catalog::Schema(column_list);
  return (schema);
}

std::shared_ptr<storage::Tuple> TypeUtilTestsHelper(catalog::Schema* schema,
                                                    int tuple_id) {
  // Populate tuple
  auto pool = TestingHarness::GetInstance().GetTestingPool();
  std::shared_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  const int num_cols = (int)schema->GetColumnCount();
  for (int i = 0; i < num_cols; i++) {
    int val = (10 * i) + tuple_id;
    auto col_type = schema->GetColumn(i).GetType();
    switch (col_type) {
      case type::TypeId::BOOLEAN: {
        bool boolVal = (tuple_id == 0);
        tuple->SetValue(i, type::ValueFactory::GetBooleanValue(boolVal), pool);
        break;
      }
      case type::TypeId::TINYINT: {
        tuple->SetValue(i, type::ValueFactory::GetTinyIntValue(val), pool);
        break;
      }
      case type::TypeId::SMALLINT: {
        tuple->SetValue(i, type::ValueFactory::GetSmallIntValue(val), pool);
        break;
      }
      case type::TypeId::INTEGER: {
        tuple->SetValue(i, type::ValueFactory::GetIntegerValue(val), pool);
        break;
      }
      case type::TypeId::BIGINT: {
        tuple->SetValue(i, type::ValueFactory::GetBigIntValue(val), pool);
        break;
      }
      case type::TypeId::DECIMAL: {
        tuple->SetValue(
            i, type::ValueFactory::GetDecimalValue(static_cast<double>(val)),
            pool);
        break;
      }
      case type::TypeId::TIMESTAMP: {
        tuple->SetValue(i, type::ValueFactory::GetTimestampValue(
                               static_cast<uint64_t>(val)),
                        pool);
        break;
      }
      case type::TypeId::VARCHAR: {
        std::ostringstream os;
        os << "TupleID=" << tuple_id << "::";
        for (int j = 0; j < 10; j++) {
          os << j;
        }
        tuple->SetValue(i, type::ValueFactory::GetVarcharValue(os.str()), pool);
        break;
      }
      default:
        throw peloton::Exception(StringUtil::Format(
            "Unexpected type %s", TypeIdToString(col_type).c_str()));
    }
  }
  return (tuple);
}

TEST_F(TypeUtilTests, CompareEqualsRawTest) {
  // This is going to be a nasty ride, so strap yourself in...
  catalog::Schema* schema = TypeUtilTestsGenerateSchema();

  std::vector<std::shared_ptr<storage::Tuple>> tuples = {
      TypeUtilTestsHelper(schema, 0),
      TypeUtilTestsHelper(schema, 1),  // Different than tuple0
      TypeUtilTestsHelper(schema, 0),  // Same as tuple0
  };
  LOG_TRACE("TUPLE0: %s", tuples[0]->GetInfo().c_str());
  LOG_TRACE("TUPLE1: %s", tuples[1]->GetInfo().c_str());
  LOG_TRACE("TUPLE2: %s", tuples[2]->GetInfo().c_str());

  const int num_cols = (int)schema->GetColumnCount();
  for (int tuple_idx = 1; tuple_idx < 3; tuple_idx++) {
    bool expected = (tuple_idx == 2);

    for (int i = 0; i < num_cols; i++) {
      const char* val0 = tuples[0]->GetDataPtr(i);
      const char* val1 = tuples[tuple_idx]->GetDataPtr(i);
      auto type = schema->GetColumn(i).GetType();
      bool inlined = schema->IsInlined(i);
      bool result = type::TypeUtil::CompareEqualsRaw(type, val0, val1, inlined);

      LOG_TRACE(
          "'%s'=='%s' => Expected:%s / Result:%s",
          tuples[0]->GetValue(i).ToString().c_str(),
          tuples[tuple_idx]->GetValue(i).ToString().c_str(),
          type::ValueFactory::GetBooleanValue(expected).ToString().c_str(),
          type::ValueFactory::GetBooleanValue(result).ToString().c_str());

      EXPECT_EQ(expected, result);
    }  // FOR (columns)
  }    // FOR (tuples)
  delete schema;
}

TEST_F(TypeUtilTests, CompareLessThanRawTest) {
  catalog::Schema* schema = TypeUtilTestsGenerateSchema();
  std::vector<std::shared_ptr<storage::Tuple>> tuples = {
      TypeUtilTestsHelper(schema, 0),
      TypeUtilTestsHelper(schema, 1),  // Different than tuple0
      TypeUtilTestsHelper(schema, 0),  // Same as tuple0
  };

  const int num_cols = (int)schema->GetColumnCount();
  for (int tuple_idx = 1; tuple_idx < 3; tuple_idx++) {
    for (int i = 0; i < num_cols; i++) {
      const char* val0 = tuples[0]->GetDataPtr(i);
      const char* val1 = tuples[tuple_idx]->GetDataPtr(i);
      auto type = schema->GetColumn(i).GetType();
      bool inlined = schema->IsInlined(i);
      bool result =
          type::TypeUtil::CompareLessThanRaw(type, val0, val1, inlined);

      auto fullVal0 = tuples[0]->GetValue(i);
      auto fullVal1 = tuples[tuple_idx]->GetValue(i);
      bool expected =
          (fullVal0.CompareLessThan(fullVal1) == type::CmpBool::CMP_TRUE);

      EXPECT_EQ(expected, result);
    }  // FOR (columns)
  }    // FOR (tuples)
  delete schema;
}

TEST_F(TypeUtilTests, CompareGreaterThanRawTest) {
  catalog::Schema* schema = TypeUtilTestsGenerateSchema();
  std::vector<std::shared_ptr<storage::Tuple>> tuples = {
      TypeUtilTestsHelper(schema, 0),
      TypeUtilTestsHelper(schema, 1),  // Different than tuple0
      TypeUtilTestsHelper(schema, 0),  // Same as tuple0
  };

  const int num_cols = (int)schema->GetColumnCount();
  for (int tuple_idx = 1; tuple_idx < 3; tuple_idx++) {
    for (int i = 0; i < num_cols; i++) {
      const char* val0 = tuples[0]->GetDataPtr(i);
      const char* val1 = tuples[tuple_idx]->GetDataPtr(i);
      auto type = schema->GetColumn(i).GetType();
      bool inlined = schema->IsInlined(i);
      bool result =
          type::TypeUtil::CompareGreaterThanRaw(type, val0, val1, inlined);

      auto fullVal0 = tuples[0]->GetValue(i);
      auto fullVal1 = tuples[tuple_idx]->GetValue(i);
      bool expected =
          (fullVal0.CompareGreaterThan(fullVal1) == type::CmpBool::CMP_TRUE);

      EXPECT_EQ(expected, result);
    }  // FOR (columns)
  }    // FOR (tuples)
  delete schema;
}
}
}
