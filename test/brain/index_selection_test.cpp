//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection_test.cpp
//
// Identification: test/brain/index_selection_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection.h"
#include "binder/bind_node_visitor.h"
#include "brain/index_selection_util.h"
#include "brain/what_if_index.h"
#include "catalog/index_catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/table_stats.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// IndexSelectionTest
//===--------------------------------------------------------------------===//

class IndexSelectionTest : public PelotonTest {
 public:
  IndexSelectionTest() {}

  // Create a new database
  void CreateDatabase(std::string db_name) {
    // Create a new database.
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(db_name, txn);
    txn_manager.CommitTransaction(txn);
  }

  // Create a new table with schema (a INT, b INT, c INT).
  void CreateTable(std::string table_name) {
    std::string create_str =
        "CREATE TABLE " + table_name + "(a INT, b INT, c INT);";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }

  void DropTable(std::string table_name) {
    std::string create_str = "DROP TABLE " + table_name + ";";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }

  void DropDatabase(std::string db_name) {
    std::string create_str = "DROP DATABASE " + db_name + ";";
    TestingSQLUtil::ExecuteSQLQuery(create_str);
  }
};

TEST_F(IndexSelectionTest, AdmissibleIndexesTest) {
  std::string table_name = "dummy_table";
  std::string database_name = DEFAULT_DB_NAME;

  CreateDatabase(database_name);
  CreateTable(table_name);

  std::vector<std::string> queries;
  std::vector<int> admissible_index_counts;

  std::ostringstream oss;
  oss << "SELECT * FROM " << table_name << " WHERE a < 1 or b > 4 GROUP BY a";
  queries.push_back(oss.str());
  admissible_index_counts.push_back(2);
  oss.str("");
  oss << "SELECT a, b, c FROM " << table_name
      << " WHERE a < 1 or b > 4 ORDER BY a";
  queries.push_back(oss.str());
  admissible_index_counts.push_back(2);
  oss.str("");
  oss << "DELETE FROM " << table_name << " WHERE a < 1 or b > 4";
  queries.push_back(oss.str());
  admissible_index_counts.push_back(2);
  oss.str("");
  oss << "UPDATE " << table_name << " SET a = 45 WHERE a < 1 or b > 4";
  queries.push_back(oss.str());
  admissible_index_counts.push_back(2);
  oss.str("");
  oss << "UPDATE " << table_name << " SET a = 45 WHERE a < 1";
  queries.push_back(oss.str());
  admissible_index_counts.push_back(1);
  oss.str("");
  oss << "SELECT a, b, c FROM " << table_name;
  queries.push_back(oss.str());
  admissible_index_counts.push_back(0);
  oss.str("");
  oss << "SELECT a, b, c FROM " << table_name << " ORDER BY a";
  queries.push_back(oss.str());
  admissible_index_counts.push_back(1);
  oss.str("");
  oss << "SELECT a, b, c FROM " << table_name << " GROUP BY a";
  queries.push_back(oss.str());
  admissible_index_counts.push_back(1);
  oss.str("");
  oss << "SELECT * FROM " << table_name;
  queries.push_back(oss.str());
  admissible_index_counts.push_back(0);
  oss.str("");

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  for (auto i = 0UL; i < queries.size(); i++) {
    // Parse the query.
    auto parser = parser::PostgresParser::GetInstance();
    std::unique_ptr<parser::SQLStatementList> stmt_list(
        parser.BuildParseTree(queries[i]).release());
    EXPECT_TRUE(stmt_list->is_valid);

    auto stmt = (parser::SelectStatement *)stmt_list->GetStatement(0);

    // Bind the query
    std::unique_ptr<binder::BindNodeVisitor> binder(
        new binder::BindNodeVisitor(txn, database_name));
    binder->BindNameToNode(stmt);

    brain::Workload w;
    w.AddQuery(stmt);

    brain::IndexSelection is(w, 5, 2, 10);
    brain::IndexConfiguration ic;
    is.GetAdmissibleIndexes(stmt, ic);

    auto indexes = ic.GetIndexes();
    EXPECT_EQ(ic.GetIndexCount(), admissible_index_counts[i]);
  }

  DropTable(table_name);
  DropDatabase(database_name);

  txn_manager.CommitTransaction(txn);
}



TEST_F(IndexSelectionTest, CandidateIndexGenerationTest) {
  std::string table_name = "dummy_table";
  std::string database_name = DEFAULT_DB_NAME;

  CreateDatabase(database_name);
  CreateTable(table_name);

  DropTable(table_name);
  DropDatabase(database_name);
}


}  // namespace test
}  // namespace peloton
