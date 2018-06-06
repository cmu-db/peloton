//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection_util.h
//
// Identification: src/include/brain/index_selection_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string.h>
#include <set>
#include <sstream>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "catalog/index_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "parser/sql_statement.h"
#include "parser/postgresparser.h"
#include "concurrency/transaction_context.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// IndexSuggestionKnobs
//===--------------------------------------------------------------------===//

// Tunable knobs of the index selection algorithm
struct IndexSelectionKnobs {
  // The number of iterations of the main algorithm which is also the maximum
  // number of columns in a single index as in ith iteration we consider indexes
  // with i or lesser columns
  size_t num_iterations_;
  // The number of indexes up to which we will do exhaustive enumeration
  size_t naive_enumeration_threshold_;
  // The number of indexes in the final configuration returned by the
  // IndexSelection algorithm
  size_t num_indexes_;
};

//===--------------------------------------------------------------------===//
// IndexObject
//===--------------------------------------------------------------------===//

// Class to represent a (hypothetical) index
struct HypotheticalIndexObject {
  // the OID of the database
  oid_t db_oid;
  // the OID of the table
  oid_t table_oid;
  // OIDs of each column in the index
  std::vector<oid_t> column_oids;

  /**
   * @brief - Constructor
   */
  HypotheticalIndexObject(){};

  /**
   * @brief - Constructor
   */
  HypotheticalIndexObject(oid_t db_oid, oid_t table_oid, oid_t col_oid)
      : db_oid(db_oid), table_oid(table_oid) {
    column_oids.push_back(col_oid);
  }

  /**
   * @brief - Constructor
   */
  HypotheticalIndexObject(oid_t db_oid, oid_t table_oid,
                          std::vector<oid_t> &col_oids)
      : db_oid(db_oid), table_oid(table_oid), column_oids(col_oids) {}

  /**
   * @brief - Equality operator of the index object
   */
  bool operator==(const HypotheticalIndexObject &obj) const;

  /**
   * @brief - Checks whether the 2 indexes can be merged to make a multi column
   * index. Return true if they are in the same database and table, else false
   */
  bool IsCompatible(std::shared_ptr<HypotheticalIndexObject> index) const;

  /**
   * @brief - Merges the 2 index objects to make a multi column index
   */
  HypotheticalIndexObject Merge(std::shared_ptr<HypotheticalIndexObject> index);

  const std::string ToString() const;
};

//===--------------------------------------------------------------------===//
// IndexConfiguration
//===--------------------------------------------------------------------===//

// Hasher for the IndexObject
struct IndexObjectHasher {
  size_t operator()(const HypotheticalIndexObject &obj) const {
    return std::hash<std::string>()(obj.ToString());
  }
};

// Call to represent a configuration - a set of hypothetical indexes
class IndexConfiguration {
 public:
  /**
   * @brief - Constructor
   */
  IndexConfiguration() {}

  /**
   * @brief - Constructor
   */
  IndexConfiguration(
      std::set<std::shared_ptr<HypotheticalIndexObject>> &index_obj_set)
      : indexes_(index_obj_set) {}

  /**
   * @brief - Merges with the argument configuration
   */
  void Merge(IndexConfiguration &config);

  /**
   * @brief replace config
   */
  void Set(IndexConfiguration &config);

  /**
   * @brief - Adds an index into the configuration
   */
  void AddIndexObject(
      const std::shared_ptr<HypotheticalIndexObject> &index_info);

  /**
   * @brief - Removes an index from the configuration
   */
  void RemoveIndexObject(
      const std::shared_ptr<HypotheticalIndexObject> &index_info);

  /**
   * @brief - Returns the number of indexes in the configuration
   */
  size_t GetIndexCount() const;

  /**
   * @brief is empty
   * @return bool
   */
  bool IsEmpty() const;

  /**
   * @brief - Returns the indexes in the configuration
   */
  const std::set<std::shared_ptr<HypotheticalIndexObject>> &GetIndexes() const;

  /**
   * @brief - Equality operator of the index configurations
   */
  bool operator==(const IndexConfiguration &obj) const;

  /**
   * @brief - Set difference of the two configurations
   */
  IndexConfiguration operator-(const IndexConfiguration &obj);

  const std::string ToString() const;

  void Clear();

 private:
  // The set of hypothetical indexes in the configuration
  std::set<std::shared_ptr<HypotheticalIndexObject>> indexes_;
};

//===--------------------------------------------------------------------===//
// IndexObjectPool
//===--------------------------------------------------------------------===//

// This class is a wrapper around a map from the IndexConfiguration to the
// shared pointer of the object. This shared pointer is used else where in the
// the algorithm to identify a configuration - memoization, enumeration,
// equality while sorting etc.
class IndexObjectPool {
 public:
  /**
   * @brief - Constructor
   */
  IndexObjectPool() {}

  /**
   * @brief - Return the shared pointer of the object from the global
   */
  std::shared_ptr<HypotheticalIndexObject> GetIndexObject(
      HypotheticalIndexObject &obj);

  /**
   * @brief - Add the object to the pool of index objects
   * if the object already exists, return the shared pointer
   * else create the object, add it to the pool and return the shared pointer
   */
  std::shared_ptr<HypotheticalIndexObject> PutIndexObject(
      HypotheticalIndexObject &obj);

 private:
  // The mapping from the object to the shared pointer
  std::unordered_map<HypotheticalIndexObject,
                     std::shared_ptr<HypotheticalIndexObject>,
                     IndexObjectHasher> map_;
};

//===--------------------------------------------------------------------===//
// Workload
//===--------------------------------------------------------------------===//

// Represents a workload of SQL queries
class Workload {
 public:
  /**
   * @brief - Constructor
   */
  Workload(std::string database_name) : database_name(database_name) {}

  /**
   * @brief - Initialize a workload with the given query strings. Parse, bind
   * and
   * add SQLStatements.
   */
  Workload(std::vector<std::string> &queries, std::string database_name,
           concurrency::TransactionContext *txn);

  /**
   * @brief - Constructor
   */
  Workload(std::pair<std::shared_ptr<parser::SQLStatement>,
                     std::unordered_set<std::string>> query,
           std::string database_name)
      : sql_queries_({query}), database_name(database_name) {}

  /**
   * @brief - Add a query into the workload
   */
  inline void AddQuery(std::shared_ptr<parser::SQLStatement> query,
                       std::unordered_set<std::string> tables) {
    sql_queries_.push_back(std::make_pair(query, tables));
  }

  /**
   * @brief - Return the queries
   */
  inline const std::vector<std::pair<std::shared_ptr<parser::SQLStatement>,
                                     std::unordered_set<std::string>>>
      &GetQueries() {
    return sql_queries_;
  }

  /**
   * @brief - Return the parsed SQLstatements
   */
  inline size_t Size() { return sql_queries_.size(); }

  /**
   * @brief Return the database name
   */
  inline std::string GetDatabaseName() {
    PELOTON_ASSERT(database_name != "");
    return database_name;
  };

  /**
   * * @brief GetTableNamesReferenced
   * Given a parsed & bound query, this function returns all the tables
   * referenced.
   * @param query - a parsed and bound SQL statement
   * @param table_names - where the table names will be stored.
   */
  static void GetTableNamesReferenced(
      std::shared_ptr<parser::SQLStatement> query,
      std::unordered_set<std::string> &table_names);

 private:
  /**
   * Parsed SQL queries along with the referenced table names.
   */
  std::vector<std::pair<std::shared_ptr<parser::SQLStatement>,
                        std::unordered_set<std::string>>> sql_queries_;
  std::string database_name;
};

}  // namespace brain
}  // namespace peloton
