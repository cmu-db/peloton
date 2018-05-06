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
  /**
   * Creates a database.
   * @param db_name
   */
  TestingIndexSuggestionUtil(std::string db_name);

  /**
   * Drops all tables and the database.
   */
  ~TestingIndexSuggestionUtil();

  /**
   * Inserts specified number of tuples.
   * @param table_name
   * @param schema schema of the table to be created
   * @param num_tuples number of tuples to be inserted with random values.
   */
  void InsertIntoTable(std::string table_name, TableSchema schema,
                       long num_tuples);

  /**
   * Create a new table.s
   * @param table_name
   * @param schema
   */
  void CreateTable(std::string table_name, TableSchema schema);


  /**
   * Factory method to create a hypothetical index object. The returned object can
   * be used
   * in the catalog or catalog cache.
   * @param table_name
   * @param index_col_names
   * @return
   */
  std::shared_ptr<brain::HypotheticalIndexObject> CreateHypotheticalIndex(
      std::string table_name, std::vector<std::string> cols);

  
  /**
   * Check whether the given indexes are the same as the expected ones
   * @param chosen_indexes
   * @param expected_indexes
   */
  bool CheckIndexes(brain::IndexConfiguration chosen_indexes,
                    std::set<std::set<oid_t>> expected_indexes);

 private:
  std::string database_name_;
  std::unordered_map<std::string, TableSchema> tables_created_;

  /**
   * Create the database
   */
  void CreateDatabase();

  /**
   * Drop the database
   */
  void DropDatabase();

  /**
   * Drop the table
   */
  void DropTable(std::string table_name);

  /**
   * Generate stats for all the tables in the system.
   */
  void GenerateTableStats();
};
}

}  // namespace test
}  // namespace peloton
