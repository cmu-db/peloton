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
#include "logging/log_buffer.h"
#include "logging/logging_util.h"
#include "util/file_util.h"
#include "type/value_factory.h"

 namespace peloton {
 namespace type {
 class ValueFactory;
}
 namespace test {

//===--------------------------------------------------------------------===//
// SerializeUtil Tests
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
    type::Value value = type::ValueFactory::GetIntegerValue(1);
    logging::LogBuffer* buf = new logging::LogBuffer();
    CopySerializeOutput output_buffer;
    size_t buf_size = 4096;
    std::unique_ptr<char[]> buffer(new char[buf_size]);
    output_buffer.Reset();
    value.SerializeTo(output_buffer);
    buf->WriteData(output_buffer.Data(), output_buffer.Size());
    FileHandle *new_file_handle = new FileHandle();

    std::string filename = "test_file";
    // Create a new file
    logging::LoggingUtil::OpenFile(filename.c_str(), "wb", *new_file_handle);
    fwrite((const void *) (buf->GetData()), buf->GetSize(), 1,
    new_file_handle->file);
    buf->Reset();

//  Call fsync
    logging::LoggingUtil::FFlushFsync(*new_file_handle);

    logging::LoggingUtil::CloseFile(*new_file_handle);

    delete new_file_handle;

    new_file_handle = new FileHandle();
    logging::LoggingUtil::OpenFile(filename.c_str(), "rb", *new_file_handle);

    logging::LoggingUtil::ReadNBytesFromFile(*new_file_handle, (void *)
    buffer.get(), output_buffer.Size());

    CopySerializeInput record_decode((const void *) buffer.get(),
    output_buffer.Size());

    type::Value val = type::Value::DeserializeFrom(record_decode,
    type::TypeId::INTEGER);
    delete new_file_handle;
    delete buf;
    EXPECT_EQ(type::CMP_TRUE,value.CompareEquals(val));

}

 TEST_F(SerializeTests, SerializeVarlenValueToFileTest) {
    type::Value value =
    type::ValueFactory::GetVarcharValue("hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello");
    logging::LogBuffer* buf = new logging::LogBuffer();
    CopySerializeOutput output_buffer;
    size_t buf_size = 4096;
    std::unique_ptr<char[]> buffer(new char[buf_size]);
    output_buffer.Reset();
    value.SerializeTo(output_buffer);
    buf->WriteData(output_buffer.Data(), output_buffer.Size());
    FileHandle *new_file_handle = new FileHandle();

    std::string filename = "test_file";
    // Create a new file
    logging::LoggingUtil::OpenFile(filename.c_str(), "wb", *new_file_handle);
    fwrite((const void *) (buf->GetData()), buf->GetSize(), 1,
    new_file_handle->file);
    buf->Reset();

//  Call fsync
    logging::LoggingUtil::FFlushFsync(*new_file_handle);

    logging::LoggingUtil::CloseFile(*new_file_handle);

    delete new_file_handle;

    new_file_handle = new FileHandle();
    logging::LoggingUtil::OpenFile(filename.c_str(), "rb", *new_file_handle);

    logging::LoggingUtil::ReadNBytesFromFile(*new_file_handle, (void *)
    buffer.get(), output_buffer.Size());

    CopySerializeInput record_decode((const void *) buffer.get(),
    output_buffer.Size());

    type::Value val = type::Value::DeserializeFrom(record_decode,
    type::TypeId::VARCHAR);
    delete new_file_handle;
    delete buf;
    EXPECT_EQ(type::CMP_TRUE,value.CompareEquals(val));
}


TEST_F(SerializeTests, SerializeVarlenValueToFileTest) {
    type::Value value = type::ValueFactory::GetVarcharValue("hellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohellohello");
    logging::LogBuffer* buf = new logging::LogBuffer();
    CopySerializeOutput output_buffer;
    size_t buf_size = 4096;
    std::unique_ptr<char[]> buffer(new char[buf_size]);
    output_buffer.Reset();
    value.SerializeTo(output_buffer);
    buf->WriteData(output_buffer.Data(), output_buffer.Size());
    FileHandle *new_file_handle = new FileHandle();

    std::string filename = "test_file";
    // Create a new file
    logging::LoggingUtil::OpenFile(filename.c_str(), "wb", *new_file_handle);
    fwrite((const void *) (buf->GetData()), buf->GetSize(), 1, new_file_handle->file);
    buf->Reset();

//  Call fsync
    logging::LoggingUtil::FFlushFsync(*new_file_handle);

    logging::LoggingUtil::CloseFile(*new_file_handle);

    delete new_file_handle;

    new_file_handle = new FileHandle();
    logging::LoggingUtil::OpenFile(filename.c_str(), "rb", *new_file_handle);

    logging::LoggingUtil::ReadNBytesFromFile(*new_file_handle, (void *) buffer.get(), output_buffer.Size());

    CopySerializeInput record_decode((const void *) buffer.get(), output_buffer.Size());

    type::Value val = type::Value::DeserializeFrom(record_decode, type::TypeId::VARCHAR);
    EXPECT_EQ(type::CMP_TRUE,value.CompareEquals(val));
}


 TEST_F(SerializeTests, SerializeValuesToFileTest) {
    auto value1 =
    type::ValueFactory::GetIntegerValue(type::PELOTON_INT32_MAX);
    auto value2 =
    type::ValueFactory::GetDecimalValue(type::PELOTON_DECIMAL_MAX);
    auto value3 =
    type::ValueFactory::GetBooleanValue(type::PELOTON_BOOLEAN_MAX);
    CopySerializeOutput output_buffer;
    output_buffer.Reset();
    value1.SerializeTo(output_buffer);
    value2.SerializeTo(output_buffer);
    value3.SerializeTo(output_buffer);

    auto filename = FileUtil::WriteTempFile(output_buffer.Data());
    tempFiles.push_back(filename);
    auto file = FileUtil::GetFile(filename);
    CopySerializeInput input_buffer(file.c_str(), output_buffer.Size());
    auto valuefinal1 = type::Value::DeserializeFrom(input_buffer,
    type::TypeId::INTEGER);
    auto valuefinal2 = type::Value::DeserializeFrom(input_buffer,
    type::TypeId::DECIMAL);
    auto valuefinal3 = type::Value::DeserializeFrom(input_buffer,
    type::TypeId::BOOLEAN);

    EXPECT_EQ(type::CMP_TRUE,value1.CompareEquals(valuefinal1));

    EXPECT_EQ(type::CMP_TRUE,value2.CompareEquals(valuefinal2));

    EXPECT_EQ(type::CMP_TRUE,value3.CompareEquals(valuefinal3));
}

 TEST_F(SerializeTests, SerializeTupleToFileTest) {
    std::vector<catalog::Column> columns;
    columns.push_back(catalog::Column(type::TypeId::INTEGER, 4, "column_a",
    true, 0));
    columns.push_back(catalog::Column(type::TypeId::DECIMAL, 8, "column_b",
    true, 4));
    columns.push_back(catalog::Column(type::TypeId::SMALLINT, 4, "column_c",
    true, 12));
    auto schema = new catalog::Schema(columns);
    auto tuple = storage::Tuple(schema, true);

    auto value1 =
    type::ValueFactory::GetIntegerValue(type::PELOTON_INT32_MAX);
    auto value2 =
    type::ValueFactory::GetDecimalValue(type::PELOTON_DECIMAL_MAX);
    auto value3 =
    type::ValueFactory::GetSmallIntValue(type::PELOTON_INT16_MIN);
    tuple.SetValue(0,value1);
    tuple.SetValue(1,value2);
    tuple.SetValue(2,value3);

    logging::LogBuffer* buf = new logging::LogBuffer();
    CopySerializeOutput output_buffer;
    size_t buf_size = 4096;
    std::unique_ptr<char[]> buffer(new char[buf_size]);
    output_buffer.Reset();
    tuple.SerializeTo(output_buffer);
    buf->WriteData(output_buffer.Data(), output_buffer.Size());
    FileHandle *new_file_handle = new FileHandle();

    std::string filename = "test_file";
    // Create a new file
    logging::LoggingUtil::OpenFile(filename.c_str(), "wb", *new_file_handle);
    fwrite((const void *) (buf->GetData()), buf->GetSize(), 1,
    new_file_handle->file);
    buf->Reset();

//  Call fsync
    logging::LoggingUtil::FFlushFsync(*new_file_handle);

    logging::LoggingUtil::CloseFile(*new_file_handle);

    delete new_file_handle;

    new_file_handle = new FileHandle();
    logging::LoggingUtil::OpenFile(filename.c_str(), "rb", *new_file_handle);

    logging::LoggingUtil::ReadNBytesFromFile(*new_file_handle, (void *)
    buffer.get(), output_buffer.Size());

    CopySerializeInput record_decode((const void *) buffer.get(),
    output_buffer.Size());
    auto tuple2 = storage::Tuple(schema, true);
    tuple2.DeserializeWithHeaderFrom(record_decode);


    EXPECT_EQ(0,tuple.Compare(tuple2));
    delete schema;
    delete buf;
    delete new_file_handle;

}  // End test namespace
}  // End peloton namespace
