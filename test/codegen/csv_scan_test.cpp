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
#include "function/date_functions.h"
#include "function/numeric_functions.h"
#include "function/string_functions.h"
#include "util/file_util.h"

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
                  CallbackFn callback, char delimiter = ',') {
  std::string csv_data;
  for (uint32_t i = 0; i < rows.size(); i++) {
    csv_data.append(rows[i]).append("\n");
  }

  // Write the contents into a temporary file
  TempFileHandle fh{FileUtil::WriteTempFile(csv_data, "", "tmp")};

  // The memory pool
  auto &pool = *TestingHarness::GetInstance().GetTestingPool();

  // The client-state
  State state = {.scanner = nullptr, .callback = callback};

  // The scanner
  codegen::util::CSVScanner scanner{
      pool, fh.name, col_types.data(), static_cast<uint32_t>(col_types.size()),
      CSVRowCallback, reinterpret_cast<void *>(&state), delimiter};

  state.scanner = &scanner;

  // Iterate!
  scanner.Produce();
}

TEST_F(CSVScanTest, SimpleNumericScan) {
  // Create a temporary CSV file
  std::vector<std::string> rows = {"1,2,3.0,4", "4,5,6.0,7", "8,9,10.0,11"};
  std::vector<codegen::type::Type> types = {{type::TypeId::INTEGER, false},
                                            {type::TypeId::INTEGER, false},
                                            {type::TypeId::DECIMAL, false},
                                            {type::TypeId::INTEGER, false}};

  uint32_t rows_read = 0;
  IterateAsCSV(rows, types, [&rows_read, &types](
                                const codegen::util::CSVScanner::Column *cols) {
    rows_read++;
    for (uint32_t i = 0; i < types.size(); i++) {
      EXPECT_FALSE(cols[i].is_null);
      EXPECT_GT(cols[i].len, 0);
    }
  });

  // Check
  EXPECT_EQ(rows.size(), rows_read);
}

TEST_F(CSVScanTest, MixedStringScan) {
  // Create a temporary CSV file
  std::vector<std::string> rows = {
      "1,1994-01-01,3,test", "4,2018-01-01,6,\"test\"",
      "8,2016-05-05,10,\"test\nnewline\ninquote\""};
  std::vector<codegen::type::Type> types = {{type::TypeId::INTEGER, false},
                                            {type::TypeId::DATE, false},
                                            {type::TypeId::INTEGER, false},
                                            {type::TypeId::VARCHAR, false}};

  std::vector<std::string> rows_read;
  IterateAsCSV(rows, types, [&rows_read, &types](
                                const codegen::util::CSVScanner::Column *cols) {
    std::string row;
    for (uint32_t i = 0; i < types.size(); i++) {
      EXPECT_FALSE(cols[i].is_null);
      EXPECT_GT(cols[i].len, 0);
      if (i > 0) row.append(",");
      switch (types[i].type_id) {
        case type::TypeId::INTEGER: {
          row.append(std::to_string(function::NumericFunctions::InputInteger(
              types[i], cols[i].ptr, cols[i].len)));
          break;
        }
        case type::TypeId::DATE: {
          auto raw_date = function::DateFunctions::InputDate(
              types[i], cols[i].ptr, cols[i].len);
          int32_t year, month, day;
          function::DateFunctions::JulianToDate(raw_date, year, month, day);
          row.append(StringUtil::Format("%u-%02u-%02u", year, month, day));
          break;
        }
        case type::TypeId::VARCHAR: {
          auto ret = function::StringFunctions::InputString(
              types[i], cols[i].ptr, cols[i].len);
          row.append(std::string{ret.str, ret.length - 1});
          break;
        }
        default: {
          throw Exception{StringUtil::Format(
              "Did not expect column type '%s' in test. Did you forget to "
              "modify the switch statement to handle a column type you've added"
              "in the test case?",
              TypeIdToString(types[i].type_id).c_str())};
        }
      }
    }
    rows_read.push_back(row);
  });

  // Check
  ASSERT_EQ(rows.size(), rows_read.size());
  for (uint32_t i = 0; i < rows.size(); i++) {
    EXPECT_EQ(rows[i], rows_read[i]);
  }
}

}  // namespace test
}  // namespace peloton