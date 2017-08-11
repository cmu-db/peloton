//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// serialize_test.cpp
//
// Identification: test/type/serialize_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <boost/filesystem.hpp>
#include <chrono>
#include <cstdio>
#include <random>

#include "common/harness.h"
#include "catalog/column.h"
#include "catalog/schema.h"

#include "executor/testing_executor_util.h"
#include "storage/tuple.h"
#include "type/serializeio.h"
#include "util/file_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// BrainUtil Tests
//===--------------------------------------------------------------------===//

class SerializeTests : public PelotonTest {
 public:
  void TearDown() override {
    for (auto path : tempFiles) {
      if (FileUtil::Exists(path)) {
        std::remove(path.c_str());
      }
    }  // FOR
    PelotonTest::TearDown();
  }
  std::vector<std::string> tempFiles;
};

TEST_F(SerializeTests, SerializeValueToFileTest) {
    auto value = type::ValueFactory::GetIntegerValue((int32_t)type::PELOTON_INT32_MAX);
    CopySerializeOutput output_buffer;
    output_buffer.Reset();
    value.SerializeTo(output_buffer);
    auto filename = FileUtil::WriteTempFile(output_buffer.Data());
    tempFiles.push_back(filename);
    auto file = FileUtil::GetFile(filename);
    CopySerializeInput input_buffer(file.c_str(), output_buffer.Size());
    auto value2 = type::Value::DeserializeFrom(input_buffer, type::TypeId::INTEGER);
    EXPECT_EQ(type::CMP_TRUE,value.CompareEquals(value2));
}

TEST_F(SerializeTests, SerializeValuesToFileTest) {
    auto value1 = type::ValueFactory::GetIntegerValue(type::PELOTON_INT32_MAX);
    auto value2 = type::ValueFactory::GetDecimalValue(type::PELOTON_DECIMAL_MAX);
    auto value3 = type::ValueFactory::GetBooleanValue(type::PELOTON_BOOLEAN_MAX);
    CopySerializeOutput output_buffer;
    output_buffer.Reset();
    value1.SerializeTo(output_buffer);
    value2.SerializeTo(output_buffer);
    value3.SerializeTo(output_buffer);

    auto filename = FileUtil::WriteTempFile(output_buffer.Data());
    tempFiles.push_back(filename);
    auto file = FileUtil::GetFile(filename);
    CopySerializeInput input_buffer(file.c_str(), output_buffer.Size());
    auto valuefinal1 = type::Value::DeserializeFrom(input_buffer, type::TypeId::INTEGER);
    auto valuefinal2 = type::Value::DeserializeFrom(input_buffer, type::TypeId::DECIMAL);
    auto valuefinal3 = type::Value::DeserializeFrom(input_buffer, type::TypeId::BOOLEAN);

    EXPECT_EQ(type::CMP_TRUE,value1.CompareEquals(valuefinal1));

    EXPECT_EQ(type::CMP_TRUE,value2.CompareEquals(valuefinal2));

    EXPECT_EQ(type::CMP_TRUE,value3.CompareEquals(valuefinal3));
}

TEST_F(SerializeTests, SerializeTupleToFileTest) {
    std::vector<catalog::Column> columns;
    columns.push_back(catalog::Column(type::TypeId::INTEGER, 4, "column_a", true, 0));
    columns.push_back(catalog::Column(type::TypeId::DECIMAL, 8, "column_b", true, 4));
    columns.push_back(catalog::Column(type::TypeId::BOOLEAN, 1, "column_b", true, 12));
    auto schema = new catalog::Schema(columns);
    auto tuple = storage::Tuple(schema, true);

    auto value1 = type::ValueFactory::GetIntegerValue(type::PELOTON_INT32_MAX);
    auto value2 = type::ValueFactory::GetDecimalValue(type::PELOTON_DECIMAL_MAX);
    auto value3 = type::ValueFactory::GetBooleanValue(type::PELOTON_BOOLEAN_MAX);
    tuple.SetValue(0,value1);
    tuple.SetValue(1,value2);
    tuple.SetValue(2,value3);

    CopySerializeOutput output_buffer;
    output_buffer.Reset();
    tuple.SerializeTo(output_buffer);
    //TODO: Understand file conversion.
    //value1.SerializeTo(output_buffer);
    //value2.SerializeTo(output_buffer);
    //value3.SerializeTo(output_buffer);

    //auto filename = FileUtil::WriteTempFile(output_buffer.Data());
    //auto file = FileUtil::GetFile(filename);
    //auto a = memcmp(file.c_str(), output_buffer.Data(), output_buffer.Size());

   // std::cout << "\nXXXXXXXXXXXX " << a << " XXXXXXX\n" << std::endl;
    //CopySerializeInput input_buffer(file.c_str(), output_buffer.Size());
     CopySerializeInput input_buffer(output_buffer.Data(), output_buffer.Size());
    auto tuple2 = storage::Tuple(schema, true);
    tuple2.DeserializeWithHeaderFrom(input_buffer);

    EXPECT_EQ(0,tuple.Compare(tuple2));
}

}  // End test namespace
}  // End peloton namespace
