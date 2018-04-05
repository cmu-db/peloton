//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tensorflow_test.cpp
//
// Identification: test/brain/tensorflow_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "catalog/index_catalog.h"
#include "brain/what_if_index.h"
#include "sql/testing_sql_util.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {

using namespace brain;
using namespace catalog;

namespace test {

//===--------------------------------------------------------------------===//
// WhatIfIndex Tests
//===--------------------------------------------------------------------===//

class WhatIfIndexTests : public PelotonTest {
private:
  std::string database_name;
public:

  WhatIfIndexTests() {
    database_name = DEFAULT_DB_NAME;
  }

  WhatIfIndexTests(std::string database_name) {
    this->database_name = database_name;
  }

  void CreateDefaultDB() {
    // Create a new database.
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(database_name, txn);
    txn_manager.CommitTransaction(txn);
  }

  void CreateTable(std::string table_name) {
    // Create a new table.
    std::ostringstream oss;
    oss << "CREATE TABLE " << table_name << "(a INT PRIMARY KEY, b INT, c INT);";
    TestingSQLUtil::ExecuteSQLQuery(oss.str());
  }

  void InsertIntoTable(std::string table_name, int no_of_tuples) {
    // Insert tuples into table
    for (int i=0; i<no_of_tuples; i++) {
      std::ostringstream oss;
      oss << "INSERT INTO "<< table_name <<" VALUES (" << i << ","
          << i + 1 << "," << i + 2 << ");";
      TestingSQLUtil::ExecuteSQLQuery(oss.str());
    }
  }

  std::shared_ptr<IndexCatalogObject> CreateHypotheticalIndex(
    std::string table_name, int col_offset) {

    // We need transaction to get table object.
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    // Get the existing table so that we can find its oid and the cols oids.
    auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
      database_name, table_name, txn);

    std::vector<oid_t> cols;
    auto col_obj_pairs = table_object->GetColumnObjects();

    // Find the column oid.
    auto offset = 0;
    for (auto it = col_obj_pairs.begin(); it != col_obj_pairs.end(); it++, offset++) {
      if (offset == col_offset) {
        cols.push_back(offset); // we just need the oid.
        break;
      }
    }
    assert(cols.size() == 1);

    // Give dummy index oid and name.
    std::ostringstream index_name_oss;
    index_name_oss << "index_" << col_offset;

    auto index_obj = std::shared_ptr<IndexCatalogObject> (
      new IndexCatalogObject(col_offset, index_name_oss.str(), table_object->GetTableOid(),
                             IndexType::BWTREE, IndexConstraintType::DEFAULT,
                             true, cols));

    txn_manager.CommitTransaction(txn);
    return index_obj;
  }
};

TEST_F(WhatIfIndexTests, BasicTest) {

  std::string table_name = "dummy_table";
  CreateDefaultDB();
  CreateTable(table_name);
  InsertIntoTable(table_name, 100);

  // Create hypothetical index objects.
  std::vector<std::shared_ptr<IndexCatalogObject>> index_objs;
  index_objs.push_back(CreateHypotheticalIndex(table_name, 1));
  //index_objs.push_back(CreateHypotheticalIndex(table_name, 2));

  // Form the query.
  std::ostringstream query_str_oss;
  query_str_oss << "SELECT a from " << table_name << " WHERE " <<
                "b < 33 AND c < 100 ORDER BY a;";

  std::unique_ptr<parser::SQLStatementList> stmt_list(
    parser::PostgresParser::ParseSQLString(query_str_oss.str()));

  // Get the optimized plan tree.
  WhatIfIndex *wif = new WhatIfIndex();
  auto result = wif->GetCostAndPlanTree(std::move(stmt_list),
                                        index_objs, DEFAULT_DB_NAME);
  delete wif;
  LOG_INFO("Cost is %lf", result->cost);
}

} // namespace test
} // namespace peloton
