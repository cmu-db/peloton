//===----------------------------------------------------------------------===//
//
//                         Peloton
//
//
// csv_scan_translator_test.cpp
//
// Identification: test/codegen/csv_scan_translator_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/testing_codegen_util.h"

#include "planner/csv_scan_plan.h"
#include "planner/insert_plan.h"
#include "planner/seq_scan_plan.h"
#include "util/string_util.h"
#include "util/file_util.h"

namespace peloton {
namespace test {

class CSVScanTranslatorTest : public PelotonCodeGenTest {
 public:
  CSVScanTranslatorTest() : PelotonCodeGenTest() {}

  oid_t TestTableId1() { return test_table_oids[0]; }
  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

 private:
  uint32_t num_rows_to_insert = 64;
};

TEST_F(CSVScanTranslatorTest, IntCsvScan) {
  // The quoting character and a helper function to quote a given string
  const char quote = '"';
  const auto quote_string = [](std::string s) {
    return StringUtil::Format("%c%s%c", quote, s.c_str(), quote);
  };

  // Test input rows
  // clang-format off
  std::vector<std::string> rows = {
      "1,2,3.9,four",
      "5,6,7.4,eight",
      "9,10,11.1," + quote_string("twelve"),
      "14,15,16.7,eighteen " + quote_string("nineteen") + " twenty " + quote_string("twenty-one")};
  // clang-format on

  std::string csv_data;
  for (const auto &row : rows) {
    csv_data.append(row).append("\n");
  }

  ///////////////////////////////////////////////////
  /// First insert contents of CSV into test table
  ///////////////////////////////////////////////////
  {
    // Write the contents into a temporary file
    TempFileHandle fh{FileUtil::WriteTempFile(csv_data, "", "tmp")};

    // clang-format off
    // NOTE: this schema has to match that of the test table!
    std::vector<planner::CSVScanPlan::ColumnInfo> cols = {
        planner::CSVScanPlan::ColumnInfo{.name = "1", .type = peloton::type::TypeId::INTEGER},
        planner::CSVScanPlan::ColumnInfo{.name = "2", .type = peloton::type::TypeId::INTEGER},
        planner::CSVScanPlan::ColumnInfo{.name = "3", .type = peloton::type::TypeId::DECIMAL},
        planner::CSVScanPlan::ColumnInfo{.name = "4", .type = peloton::type::TypeId::VARCHAR},
    };
    // clang-format on
    std::unique_ptr<planner::AbstractPlan> csv_scan{
        new planner::CSVScanPlan(fh.name, std::move(cols), ',')};
    std::unique_ptr<planner::AbstractPlan> insert{
        new planner::InsertPlan(&GetTestTable(TestTableId1()))};

    insert->AddChild(std::move(csv_scan));

    planner::BindingContext ctx;
    insert->PerformBinding(ctx);

    codegen::BufferingConsumer consumer{{0, 1, 2, 3}, ctx};

    // Execute insert
    CompileAndExecute(*insert, consumer);
    ASSERT_EQ(0, consumer.GetOutputTuples().size());
  }

  ///////////////////////////////////////////////////
  /// Now scan test table, comparing results
  ///////////////////////////////////////////////////
  {
    std::unique_ptr<planner::AbstractPlan> scan{new planner::SeqScanPlan(
        &GetTestTable(TestTableId1()), nullptr, {0, 1, 2, 3})};

    planner::BindingContext ctx;
    scan->PerformBinding(ctx);

    codegen::BufferingConsumer consumer{{0, 1, 2, 3}, ctx};

    // Execute insert
    CompileAndExecute(*scan, consumer);

    const auto &output = consumer.GetOutputTuples();
    ASSERT_EQ(rows.size(), output.size());
    for (uint32_t i = 0; i < rows.size(); i++) {
      EXPECT_EQ(StringUtil::Strip(rows[i], '"'), output[i].ToCSV());
    }
  }
}

}  // namespace test
}  // namespace peloton
