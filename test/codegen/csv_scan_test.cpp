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

namespace peloton {
namespace test {

class CSVScanTest : public PelotonCodeGenTest {};

using CallbackFn =
    std::function<void(const codegen::util::CSVScanner::Column *)>;

struct State {
  codegen::util::CSVScanner *scanner;
  CallbackFn callback;
};

struct TempFileHandle {
  std::string name;
  TempFileHandle(std::string _name) : name(_name) {}
  ~TempFileHandle() { boost::filesystem::remove(name); }
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
  std::vector<std::string> rows = {"1,2,3,test", "4,5,6,\"test\"",
                                   "8,9,10,\"test\nnewline\ninquote\""};
  std::vector<codegen::type::Type> types = {{type::TypeId::INTEGER, false},
                                            {type::TypeId::INTEGER, false},
                                            {type::TypeId::INTEGER, false},
                                            {type::TypeId::VARCHAR, false}};

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

}  // namespace test
}  // namespace peloton