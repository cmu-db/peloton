//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// binder_test.cpp
//
// Identification: test/binder/binder_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "common/harness.h"
#include "common/statement.h"
#include "expression/tuple_value_expression.h"
#include "binder/bind_node_visitor.h"
#include "parser/parser.h"
#include "optimizer/simple_optimizer.h"
#include "tcop/tcop.h"

using std::string;
using std::unique_ptr;
using std::vector;
using std::make_tuple;

namespace peloton {
namespace test {

class BinderCorrectnessTest : public PelotonTest {};

TEST_F(BinderCorrectnessTest, SelectStatementTest) {
  LOG_INFO("Creating default database");
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, nullptr);
  LOG_INFO("Default database created!");

  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto& parser = parser::Parser::GetInstance();
  auto& traffic_cop = tcop::TrafficCop::GetInstance();
  catalog::Catalog* catalog_ptr = catalog::Catalog::GetInstance();
  optimizer::SimpleOptimizer optimizer;
  

  vector<string> createTableSQLs{"CREATE TABLE A(A1 int, a2 varchar)",
                                  "CREATE TABLE b(B1 int, b2 varchar)"};
  auto txn = txn_manager.BeginTransaction();
  for (auto& sql : createTableSQLs) {
    LOG_INFO("Creating test table");
    vector<type::Value> params;
    vector<StatementResult> result;
    vector<int> result_format;
    unique_ptr<Statement> statement(new Statement("CREATE", sql));
    auto parse_tree = parser.BuildParseTree(sql);
    statement->SetPlanTree(optimizer.BuildPelotonPlanTree(parse_tree));
    auto status = traffic_cop.ExecuteStatementPlan(parse_tree->GetStatements().at(0), params, result, result_format);
    LOG_INFO("Table create result: %s", ResultTypeToString(status.m_result).c_str());
  }
  txn_manager.CommitTransaction(txn);
  
  // Test regular table name
  auto binder = std::make_unique<binder::BindNodeVisitor>();
  string selectSQL = "SELECT A.a1, B.b2 FROM A INNER JOIN b ON a.a1 = b.b1
                     "WHERE a1 < 100 GROUP BY A.a1, B.b2 HAVING a1 > 50
                     "ORDER BY a1;";
  parser::SelectStatement* selectStmt = parser.BuildParseTree(selectSQL)->GetStatements()->at(0);
  binder->BindNameToNode(selectStmt);
  
  oid_t db_oid = catalog_ptr->GetDatabaseWithName(DEFAULT_DB_NAME)->GetOid();
  oid_t tableA_oid = catalog_ptr->GetTableWithName(DEFAULT_DB_NAME, "A")->GetOid();
  oid_t tableB_oid = catalog_ptr->GetTableWithName(DEFAULT_DB_NAME, "B")->GetOid();
  
  // Check select_list
  oids = (expression::TupleValueExpression*)(selectStmt->select_list[0])->BoundObjectId;
  EXPECT_EQ(oids, make_tuple(db_oid, tableA_oid, 0)); // A.a1
  oids = (expression::TupleValueExpression*)(selectStmt->select_list[1])->BoundObjectId;
  EXPECT_EQ(oids, make_tuple(db_oid, tableB_oid, 1)); // B.b2


  // Check join condition
  auto condition = (expression::ComparisonExpression*)(selectStmt->from_table->join->condition)
  auto oids = (expression::TupleValueExpression*)(condition->left)->BoundObjectId;
  EXPECT_EQ(oids, make_tuple(db_oid, tableA_oid, 0)); // a.a1
  oids = (expression::TupleValueExpression*)(condition->left)->BoundObjectId;
  EXPEXT_EQ(oids, make_tuple(db_oid, tableB_oid, 0)); // b.b1
  
  // Check Where clause
  oids = (expression::TupleValueExpression*)(selectStmt->group_by->children_[0])->BoundObjectId;
  EXPECT_EQ(oids, make_tuple(db_oid, tableA_oid, 0)); // a1
  
  // Check Group By and Having
  oids = (expression::TupleValueExpression*)(selectStmt->group_by->columns[0])->BoundObjectId;
  EXPEXT_EQ(oids, make_tuple(db_oid, tableA_oid, 0)); // A.a1
  oids = (expression::TupleValueExpression*)(selectStmt->group_by->columns[1])->BoundObjectId;
  EXPEXT_EQ(oids, make_tuple(db_oid, tableB_oid, 1)); // B.b2
  oids = (expression::TupleValueExpression*)(selectStmt->group_by.having)->BoundObjectId;
  EXPECT_EQ(oids, make_tuple(db_oid, tableA_oid, 0)); // a1

  // Check Order By
  oids = (expression::TupleValueExpression*)(selectStmt->order->expr)->BoundObjectId;
  EXPECT_EQ(oids, make_tuple(db_oid, tableA_oid, 0)); // a1
  
  // TODO: Test alias ambiguous
  // TODO: Test alias and select_list
  
  // Delete the test database
  catalog_ptr->DropDatabaseWithName(DEFAULT_DB_NAME, nullptr);
}
} // End test namespace
} // End peloton namespace