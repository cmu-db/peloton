//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scan_test.cpp
//
// Identification: test/codegen/csv_scan_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/testing_codegen_util.h"

#include "codegen/util/csv_scanner.h"
#include "common/timer.h"
#include "util/file_util.h"
#include "util/string_util.h"

namespace peloton {
namespace test {

class CSVScanTest : public PelotonCodeGenTest {};

using CallbackFn =
    std::function<void(const codegen::util::CSVScanner::Column *)>;

struct State {
  codegen::util::CSVScanner *scanner;
  CallbackFn callback;
};

void CSVRowCallback(void *s) {
  auto *state = reinterpret_cast<State *>(s);
  state->callback(state->scanner->GetColumns());
}

void IterateAsCSV(const std::vector<std::string> &rows,
                  const std::vector<codegen::type::Type> &col_types,
                  CallbackFn callback, char delimiter = ',', char quote = '"',
                  char escape = '"') {
  std::string csv_data;
  for (const auto &row : rows) {
    csv_data.append(row).append("\n");
  }

  // Write the contents into a temporary file
  TempFileHandle fh(FileUtil::WriteTempFile(csv_data, "", "tmp"));

  // The memory pool
  auto &pool = *TestingHarness::GetInstance().GetTestingPool();

  // The client-state
  State state = {.scanner = nullptr, .callback = callback};

  // The scanner
  codegen::util::CSVScanner scanner(
      pool, fh.name, col_types.data(), static_cast<uint32_t>(col_types.size()),
      CSVRowCallback, reinterpret_cast<void *>(&state), delimiter, quote,
      escape);

  state.scanner = &scanner;

  // Iterate!
  scanner.Produce();
}

TEST_F(CSVScanTest, NumericScanTest) {
  // The set of test rows and their types
  std::vector<std::string> rows = {"1,2,3.0,4", "4,5,6.0,7", "8,9,10.0,11"};
  std::vector<codegen::type::Type> types = {{type::TypeId::INTEGER, false},
                                            {type::TypeId::INTEGER, false},
                                            {type::TypeId::DECIMAL, false},
                                            {type::TypeId::INTEGER, false}};

  uint32_t rows_read = 0;
  IterateAsCSV(rows, types, [&rows, &rows_read, &types](
                                const codegen::util::CSVScanner::Column *cols) {
    // Split the input row into column values
    const auto input_parts = StringUtil::Split(rows[rows_read++], ',');

    // Check contents of row based on test input
    for (uint32_t i = 0; i < types.size(); i++) {
      // The column isn't null
      EXPECT_FALSE(cols[i].is_null);

      // The column has a value
      EXPECT_GT(cols[i].len, 0);

      // Check the string representations
      EXPECT_EQ(input_parts[i], std::string(cols[i].ptr, cols[i].len));
    }
  });

  EXPECT_EQ(rows.size(), rows_read);
}

TEST_F(CSVScanTest, QuoteEscapeTest) {
  // The set of test rows and their types
  std::vector<std::string> rows = {"yea he's \"cool\",1,2", "a quote:\"\",3,4"};
  std::vector<codegen::type::Type> types = {{type::TypeId::VARCHAR, false},
                                            {type::TypeId::INTEGER, false},
                                            {type::TypeId::INTEGER, false}};

  uint32_t rows_read = 0;
  IterateAsCSV(rows, types, [&rows, &rows_read, &types](
                                const codegen::util::CSVScanner::Column *cols) {
    // Split the input row into column values
    auto input_parts = StringUtil::Split(rows[rows_read++], ',');

    // Check contents of row based on test input
    for (uint32_t i = 0; i < types.size(); i++) {
      // The column isn't null
      EXPECT_FALSE(cols[i].is_null);

      // The column has a value
      EXPECT_GT(cols[i].len, 0);

      // Check the string representations. We need to strip off any quotes from
      // the original string since the CSV scan will strip them for us.
      EXPECT_EQ(StringUtil::Strip(input_parts[i], '"'),
                std::string(cols[i].ptr, cols[i].len));
    }
  });

  EXPECT_EQ(rows.size(), rows_read);
}

TEST_F(CSVScanTest, MixedStringTest) {
  std::vector<std::string> rows = {
      "1,1994-01-01,3,test", "4,2018-01-01,6,\"quoted_test\"",
      "8,2016-05-05,10,\"test\nnewline\ninquote\""};
  std::vector<codegen::type::Type> types = {{type::TypeId::INTEGER, false},
                                            {type::TypeId::DATE, false},
                                            {type::TypeId::INTEGER, false},
                                            {type::TypeId::VARCHAR, false}};
  uint32_t rows_read = 0;
  IterateAsCSV(rows, types, [&rows, &rows_read, &types](
                                const codegen::util::CSVScanner::Column *cols) {
    // Split the input row into column values
    auto input_parts = StringUtil::Split(rows[rows_read++], ',');

    for (uint32_t i = 0; i < types.size(); i++) {
      // The column isn't null
      EXPECT_FALSE(cols[i].is_null);

      // The column has a value
      EXPECT_GT(cols[i].len, 0);

      // Check the string representations. We need to strip off any quotes from
      // the original string since the CSV scan will strip them for us.
      EXPECT_EQ(StringUtil::Strip(input_parts[i], '"'),
                std::string(cols[i].ptr, cols[i].len));
    }
  });

  EXPECT_EQ(rows.size(), rows_read);
}

TEST_F(CSVScanTest, CatchErrorsTest) {
  ////////////////////////////////////////////////////////////////////
  ///
  /// Test Case - Missing last column
  ///
  ////////////////////////////////////////////////////////////////////
  {
    std::vector<std::string> missing_col = {"1,1994-01-01,3"};
    std::vector<codegen::type::Type> types = {{type::TypeId::INTEGER, false},
                                              {type::TypeId::DATE, false},
                                              {type::TypeId::INTEGER, false},
                                              {type::TypeId::VARCHAR, false}};
    EXPECT_ANY_THROW(IterateAsCSV(
        missing_col, types,
        [](UNUSED_ATTRIBUTE const codegen::util::CSVScanner::Column *cols) {
          FAIL();
        }));
  }

  ////////////////////////////////////////////////////////////////////
  ///
  /// Test Case - Unclosed quote
  ///
  ////////////////////////////////////////////////////////////////////
  {
    std::vector<std::string> missing_col = {"1,\"unclosed,3"};
    std::vector<codegen::type::Type> types = {{type::TypeId::INTEGER, false},
                                              {type::TypeId::VARCHAR, false},
                                              {type::TypeId::INTEGER, false}};
    EXPECT_ANY_THROW(IterateAsCSV(
        missing_col, types,
        [](UNUSED_ATTRIBUTE const codegen::util::CSVScanner::Column *cols) {
          FAIL();
        }));
  }

  ////////////////////////////////////////////////////////////////////
  ///
  /// Test Case - Unclosed quote
  ///
  ////////////////////////////////////////////////////////////////////
  {
    std::vector<std::string> missing_col = {"1,unclosed\",3"};
    std::vector<codegen::type::Type> types = {{type::TypeId::INTEGER, false},
                                              {type::TypeId::VARCHAR, false},
                                              {type::TypeId::INTEGER, false}};
    EXPECT_ANY_THROW(IterateAsCSV(
        missing_col, types,
        [](UNUSED_ATTRIBUTE const codegen::util::CSVScanner::Column *cols) {
          FAIL();
        }));
  }
}

}  // namespace test
}  // namespace peloton