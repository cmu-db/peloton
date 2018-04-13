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

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// IndexObject
//===--------------------------------------------------------------------===//

// Class to represent a (hypothetical) index
struct IndexObject {
  // the OID of the database
  oid_t db_oid;
  // the OID of the table
  oid_t table_oid;
  // OIDs of each column in the index
  std::set<oid_t> column_oids;

  /**
   * @brief - Constructor
   */
  IndexObject(){};

  /**
   * @brief - Constructor
   */
  IndexObject(oid_t db_oid, oid_t table_oid, oid_t col_oid)
      : db_oid(db_oid), table_oid(table_oid) {
    column_oids.insert(col_oid);
  }

  /**
   * @brief - Constructor
   */
  IndexObject(oid_t db_oid, oid_t table_oid, std::vector<oid_t> &col_oids)
      : db_oid(db_oid), table_oid(table_oid) {
    for (auto col : col_oids) column_oids.insert(col);
  }

  /**
   * @brief - Equality operator of the index object
   */
  bool operator==(const IndexObject &obj) const;

  /**
   * @brief - Checks whether the 2 indexes can be merged to make a multi column
   * index
   */
  bool IsCompatible(std::shared_ptr<IndexObject> index) const;

  /**
   * @brief - Merges the 2 index objects to make a multi column index
   */
  IndexObject Merge(std::shared_ptr<IndexObject> index);

  const std::string ToString() const;
};

//===--------------------------------------------------------------------===//
// IndexConfiguration
//===--------------------------------------------------------------------===//

// Hasher for the IndexObject
struct IndexObjectHasher {
  size_t operator()(const IndexObject &obj) const {
    return std::hash<std::string>()(obj.ToString());
  }
};

// Call to represent a configuration - a set of hypothetical indexes
class IndexConfiguration {
 public:
  /**
   * @brief - Constructor
   */
  IndexConfiguration();

  /**
   * @brief - Constructor
   */
  IndexConfiguration(std::set<std::shared_ptr<IndexObject>> &index_obj_set)
      : indexes_(index_obj_set) {}

  /**
   * @brief - Merges with the argument configuration
   */
  void Merge(IndexConfiguration &config);

  /**
   * @brief - Adds an index into the configuration
   */
  void AddIndexObject(std::shared_ptr<IndexObject> index_info);

  /**
   * @brief - Removes an index from the configuration
   */
  void RemoveIndexObject(std::shared_ptr<IndexObject> index_info);

  /**
   * @brief - Returns the number of indexes in the configuration
   */
  size_t GetIndexCount() const;

  /**
   * @brief - Returns the indexes in the configuration
   */
  const std::set<std::shared_ptr<IndexObject>> &GetIndexes() const;

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
  std::set<std::shared_ptr<IndexObject>> indexes_;
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
  IndexObjectPool();

  /**
   * @brief - Return the shared pointer of the object from the global
   */
  std::shared_ptr<IndexObject> GetIndexObject(IndexObject &obj);

  /**
   * @brief - Constructor
   */
  std::shared_ptr<IndexObject> PutIndexObject(IndexObject &obj);

 private:
  // The mapping from the object to the shared pointer
  std::unordered_map<IndexObject, std::shared_ptr<IndexObject>,
                     IndexObjectHasher>
      map_;
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
  Workload() {}

  /**
   * @brief - Initialize a workload with the given query strings. Parse, bind and
   * add SQLStatements.
   */
  Workload(std::vector<std::string> &queries, std::string database_name) {

    LOG_DEBUG("Initializing workload with %ld queries", queries.size());

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto parser = parser::PostgresParser::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    std::unique_ptr<binder::BindNodeVisitor> binder(
      new binder::BindNodeVisitor(txn, database_name));

    // Parse and bind every query. Store the results in the workload vector.
    for (auto it = queries.begin(); it != queries.end(); it++) {
      auto query = *it;
      LOG_INFO("Query: %s", query.c_str());

      auto stmt_list = parser::PostgresParser::ParseSQLString(query);
      PELOTON_ASSERT(stmt_list->is_valid);

      auto stmt = stmt_list->GetStatement(0);
      PELOTON_ASSERT(stmt->GetType() != StatementType::INVALID);

      // Bind the query
      binder->BindNameToNode(stmt);

      AddQuery(stmt);
    }

    txn_manager.CommitTransaction(txn);
  }

  /**
   * @brief - Constructor
   */
  Workload(parser::SQLStatement *query) : sql_queries_({query}) {}

  /**
   * @brief - Add a query into the workload
   */
  void AddQuery(parser::SQLStatement *query) { sql_queries_.push_back(query); }

  /**
   * @brief - Return the queries
   */
  const std::vector<parser::SQLStatement *> &GetQueries() {
    return sql_queries_;
  }

  /**
   * @brief - Return the parsed SQLstatements
   */
  size_t Size() { return sql_queries_.size(); }

 private:
  // A vertor of the parsed SQLStatements of the queries
  std::vector<parser::SQLStatement *> sql_queries_;
};

}  // namespace brain
}  // namespace peloton
