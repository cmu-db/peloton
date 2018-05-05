//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_index_suggestion_util.h
//
// Identification: test/include/brain/testing_index_suggestion_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "brain/index_selection_util.h"

namespace peloton {
namespace test {

namespace index_suggestion {

/**
 * Table column type.
 */
enum TupleValueType { INTEGER, FLOAT, STRING };

/**
 * Represents the schema for creating tables in the test cases.
 */
class TableSchema {
 public:
  std::vector<std::pair<std::string, TupleValueType>> cols;
  std::unordered_map<std::string, long> col_offset_map;
  TableSchema(std::vector<std::pair<std::string, TupleValueType>> columns) {
    auto i = 0UL;
    for (auto col : columns) {
      cols.push_back(col);
      col_offset_map[col.first] = i;
      i++;
    }
  }
};

/**
 * Utility class for testing Index Selection (auto-index).
 */
class TestingIndexSuggestionUtil {
 public:
  TestingIndexSuggestionUtil(std::string db_name);
  ~TestingIndexSuggestionUtil();

  // Inserts specified number of tuples into the table with random values.
  void InsertIntoTable(std::string table_name, TableSchema schema,
                                long num_tuples);

  // Creates a new table with the provided schema.
  void CreateTable(std::string table_name, TableSchema schema);

  // Factory method
  // Returns a what-if index on the columns at the given
  // offset of the table.
  std::shared_ptr<brain::HypotheticalIndexObject> CreateHypotheticalIndex(
      std::string table_name, std::vector<std::string> cols);


 private:
  std::string database_name_;
  std::unordered_map<std::string, TableSchema> tables_created_;

  void CreateDatabase();
  void DropDatabase();
  void DropTable(std::string table_name);
  void GenerateTableStats();
};
}

}  // namespace test
}  // namespace peloton
