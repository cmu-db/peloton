//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// testing_index_selection_util.h
//
// Identification: test/include/brain/testing_index_selection_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "brain/index_selection_util.h"
#include "brain/index_selection.h"
#include "brain/what_if_index.h"

namespace peloton {
namespace test {

namespace index_selection {

/**
 * Table column type.
 */
enum class TupleValueType { INTEGER, FLOAT, STRING, INTEGERPKEY };

/**
 * Represents workload types used in the test cases.
 */
enum class QueryStringsWorkloadType { SingleTableTwoColW1, SingleTableTwoColW2,
  SingleTableThreeColW, MultiTableMultiColW, SingleTableNoop, MultiTableNoop };

/**
 * Represents the schema for creating tables in the test cases.
 */
class TableSchema {
 public:
  std::vector<std::pair<std::string, TupleValueType>> cols;
  std::unordered_map<std::string, long> col_offset_map;
  std::string table_name;

  TableSchema(){};
  TableSchema(std::string table_name,
              std::vector<std::pair<std::string, TupleValueType>> columns) {
    auto i = 0UL;
    for (auto col : columns) {
      cols.push_back(col);
      col_offset_map[col.first] = i;
      i++;
    }
    this->table_name = table_name;
  }
};

/**
 * Utility class for testing Index Selection (auto-index).
 */
class TestingIndexSelectionUtil {
 public:
  /**
   * Creates a database.
   * @param db_name
   */
  TestingIndexSelectionUtil(std::string db_name);

  /**
   * Drops all tables and the database.
   */
  ~TestingIndexSelectionUtil();

  /**
   * Inserts specified number of tuples.
   * @param schema schema of the table to be created
   * @param num_tuples number of tuples to be inserted with random values.
   */
  void InsertIntoTable(TableSchema schema, long num_tuples);

  /**
   * Create a new table.s
   * @param schema
   */
  void CreateTable(TableSchema schema);

  /**
   * Factory method to create a hypothetical index object. The returned object
   * can be used in the catalog or catalog cache.
   * @param table_name
   * @param index_col_names
   * @return
   */
  std::shared_ptr<brain::HypotheticalIndexObject> CreateHypotheticalIndex(
      std::string table_name, std::vector<std::string> cols,
      brain::IndexSelection *is = nullptr);

  /**
   * Given a hypothetical index object, this method creates an actual index
   * corresponding to that hypothetical index object
   * @param index_obj: hypothetical index object
   */
  void CreateIndex(std::shared_ptr<brain::HypotheticalIndexObject> index_obj);

  /**
   * Return a micro workload
   * This function returns queries and the respective table schemas
   * User of this function must create all of the returned tables.
   * @param workload_type type of the workload to be returned
   * @return workload query strings along with the table schema
   */
  std::pair<std::vector<TableSchema>, std::vector<std::string>>
  GetQueryStringsWorkload(QueryStringsWorkloadType workload_type);

  /**
   * Return a cyclic workload
   * This function accepts a list of workload types and number of
   * workload cycles and returns the representative cyclic workload
   * eg. ((W1, W2), 3) -> (W1, W2, W1, W2, W1, W2)
   * @param workload_types sequence of the workloads - you can assume one cycle involves running such a sequence
   * @return workload query strings along with the table schema
   */
  std::pair<std::vector<TableSchema>, std::vector<std::string>>
  GetCyclicWorkload(std::vector<QueryStringsWorkloadType> workload_types, int num_cycles);

  /**
   * Get the an estimate of cost of running a query on a given
   * index configuration by the cost model(Available via What-If API)
   * @param query: the query string
   * @param config: Index configuration
   * @param database_name: DB name
   * @return: "What-If" Index cost
   */
  double WhatIfIndexCost(std::string query,
                         brain::IndexConfiguration &config,
                         std::string database_name);

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
