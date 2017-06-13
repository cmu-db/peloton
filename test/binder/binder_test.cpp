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
#include "parser/postgresparser.h"

#include "optimizer/simple_optimizer.h"
#include "tcop/tcop.h"

#include <memory>

using std::string;
using std::unique_ptr;
using std::vector;
using std::make_tuple;

namespace peloton {
namespace test {

class BinderCorrectnessTest : public PelotonTest {};

void SetupTables() {
  LOG_INFO("Creating default database");
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
  LOG_INFO("Default database created!");

  auto& parser = parser::PostgresParser::GetInstance();
  auto& traffic_cop = tcop::TrafficCop::GetInstance();
  optimizer::SimpleOptimizer optimizer;

  txn = txn_manager.BeginTransaction();
  vector<string> createTableSQLs{"CREATE TABLE A(A1 int, a2 varchar)",
                                 "CREATE TABLE b(B1 int, b2 varchar)"};
  for (auto& sql : createTableSQLs) {
    LOG_INFO("%s", sql.c_str());
    vector<type::Value> params;
    vector<StatementResult> result;
    vector<int> result_format;
    unique_ptr<Statement> statement(new Statement("CREATE", sql));
    auto parse_tree = parser.BuildParseTree(sql);
    statement->SetPlanTree(optimizer.BuildPelotonPlanTree(parse_tree));
    auto status = traffic_cop.ExecuteStatementPlan(
        statement->GetPlanTree().get(), params, result, result_format);
    LOG_INFO("Table create result: %s",
             ResultTypeToString(status.m_result).c_str());
  }
  txn_manager.CommitTransaction(txn);
}

TEST_F(BinderCorrectnessTest, SelectStatementTest) {
  SetupTables();
  auto& parser = parser::PostgresParser::GetInstance();
  catalog::Catalog* catalog_ptr = catalog::Catalog::GetInstance();

  // Test regular table name
  LOG_INFO("Parsing sql query");
  unique_ptr<binder::BindNodeVisitor> binder(new binder::BindNodeVisitor());
  string selectSQL =
      "SELECT A.a1, B.b2 FROM A INNER JOIN b ON a.a1 = b.b1 "
      "WHERE a1 < 100 GROUP BY A.a1, B.b2 HAVING a1 > 50 "
      "ORDER BY a1";

  auto parse_tree = parser.BuildParseTree(selectSQL);
  auto selectStmt =
      dynamic_cast<parser::SelectStatement*>(parse_tree->GetStatements().at(0));
  binder->BindNameToNode(selectStmt);

  oid_t db_oid = catalog_ptr->GetDatabaseWithName(DEFAULT_DB_NAME)->GetOid();
  oid_t tableA_oid =
      catalog_ptr->GetTableWithName(DEFAULT_DB_NAME, "a")->GetOid();
  oid_t tableB_oid =
      catalog_ptr->GetTableWithName(DEFAULT_DB_NAME, "b")->GetOid();

  // Check select_list
  LOG_INFO("Checking select list");
  auto tupleExpr =
      (expression::TupleValueExpression*)(*selectStmt->select_list)[0];
  EXPECT_EQ(tupleExpr->GetBoundOid(),
            make_tuple(db_oid, tableA_oid, 0));  // A.a1
  EXPECT_EQ(type::TypeId::INTEGER, tupleExpr->GetValueType());
  tupleExpr = (expression::TupleValueExpression*)(*selectStmt->select_list)[1];
  EXPECT_EQ(tupleExpr->GetBoundOid(),
            make_tuple(db_oid, tableB_oid, 1));  // B.b2
  EXPECT_EQ(type::TypeId::VARCHAR, tupleExpr->GetValueType());
  

  // Check join condition
  LOG_INFO("Checking join condition");
  tupleExpr = (expression::TupleValueExpression*)
                  selectStmt->from_table->join->condition->GetChild(0);
  EXPECT_EQ(tupleExpr->GetBoundOid(),
            make_tuple(db_oid, tableA_oid, 0));  // a.a1
  tupleExpr = (expression::TupleValueExpression*)
                  selectStmt->from_table->join->condition->GetChild(1);
  EXPECT_EQ(tupleExpr->GetBoundOid(),
            make_tuple(db_oid, tableB_oid, 0));  // b.b1

  // Check Where clause
  LOG_INFO("Checking where clause");
  tupleExpr =
      (expression::TupleValueExpression*)selectStmt->where_clause->GetChild(0);
  EXPECT_EQ(tupleExpr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));  // a1

  // Check Group By and Having
  LOG_INFO("Checking group by");
  tupleExpr =
      (expression::TupleValueExpression*)selectStmt->group_by->columns->at(0);
  EXPECT_EQ(tupleExpr->GetBoundOid(),
            make_tuple(db_oid, tableA_oid, 0));  // A.a1
  tupleExpr =
      (expression::TupleValueExpression*)selectStmt->group_by->columns->at(1);
  EXPECT_EQ(tupleExpr->GetBoundOid(),
            make_tuple(db_oid, tableB_oid, 1));  // B.b2
  tupleExpr =
      (expression::TupleValueExpression*)selectStmt->group_by->having->GetChild(
          0);
  EXPECT_EQ(tupleExpr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));  // a1

  // Check Order By
  LOG_INFO("Checking order by");
  tupleExpr =
      (expression::TupleValueExpression*)selectStmt->order->exprs->at(0);
  EXPECT_EQ(tupleExpr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));  // a1

  // Check alias ambiguous
  LOG_INFO("Checking duplicate alias and table name.");
  binder.reset(new binder::BindNodeVisitor());
  selectSQL = "SELECT * FROM A, B as A";
  parse_tree = parser.BuildParseTree(selectSQL);
  selectStmt = (parser::SelectStatement*)(parse_tree->GetStatements().at(0));
  try {
    binder->BindNameToNode(selectStmt);
    EXPECT_TRUE(false);
  } catch (Exception& e) {
    LOG_INFO("Correct! Exception(%s) catched", e.what());
  }

  // Test select from different table instances from the same physical schema
  binder.reset(new binder::BindNodeVisitor());
  selectSQL = "SELECT * FROM A, A as AA where A.a1 = AA.a2";
  parse_tree = parser.BuildParseTree(selectSQL);
  selectStmt = (parser::SelectStatement*)(parse_tree->GetStatements().at(0));
  binder->BindNameToNode(selectStmt);
  LOG_INFO("Checking where clause");
  tupleExpr =
      (expression::TupleValueExpression*)selectStmt->where_clause->GetChild(0);
  EXPECT_EQ(tupleExpr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));  // a1
  tupleExpr =
      (expression::TupleValueExpression*)selectStmt->where_clause->GetChild(1);
  EXPECT_EQ(tupleExpr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 1));  // a1

  // Test alias and select_list
  LOG_INFO("Checking select_list and table alias binding");
  binder.reset(new binder::BindNodeVisitor());
  selectSQL = "SELECT AA.a1, b2 FROM A as AA, B WHERE AA.a1 = B.b1";
  parse_tree = parser.BuildParseTree(selectSQL);
  selectStmt = (parser::SelectStatement*)(parse_tree->GetStatements().at(0));
  binder->BindNameToNode(selectStmt);
  tupleExpr =
      (expression::TupleValueExpression*)(selectStmt->select_list->at(0));
  EXPECT_EQ(tupleExpr->GetBoundOid(), make_tuple(db_oid, tableA_oid, 0));
  tupleExpr =
      (expression::TupleValueExpression*)(selectStmt->select_list->at(1));
  EXPECT_EQ(tupleExpr->GetBoundOid(), make_tuple(db_oid, tableB_oid, 1));

  // Delete the test database
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog_ptr->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}

// TODO: add test for Update Statement. Currently UpdateStatement uses char*
// instead of TupleValueExpression to represent column. We can only add this
// test after UpdateStatement is changed

TEST_F(BinderCorrectnessTest, DeleteStatementTest) {
  SetupTables();
  auto& parser = parser::PostgresParser::GetInstance();
  catalog::Catalog* catalog_ptr = catalog::Catalog::GetInstance();

  oid_t db_oid = catalog_ptr->GetDatabaseWithName(DEFAULT_DB_NAME)->GetOid();
  oid_t tableB_oid =
      catalog_ptr->GetTableWithName(DEFAULT_DB_NAME, "b")->GetOid();

  string deleteSQL = "DELETE FROM b WHERE 1 = b1 AND b2 = 'str'";
  unique_ptr<binder::BindNodeVisitor> binder(new binder::BindNodeVisitor());
  auto parse_tree = parser.BuildParseTree(deleteSQL);
  auto deleteStmt =
      dynamic_cast<parser::DeleteStatement*>(parse_tree->GetStatements().at(0));
  binder->BindNameToNode(deleteStmt);

  LOG_INFO("Checking first condition in where clause");
  auto tupleExpr =
      (expression::TupleValueExpression*)deleteStmt->expr->GetChild(0)
          ->GetChild(1);
  EXPECT_EQ(tupleExpr->GetBoundOid(), make_tuple(db_oid, tableB_oid, 0));

  LOG_INFO("Checking second condition in where clause");
  tupleExpr = (expression::TupleValueExpression*)deleteStmt->expr->GetChild(1)
                  ->GetChild(0);
  EXPECT_EQ(tupleExpr->GetBoundOid(), make_tuple(db_oid, tableB_oid, 1));

  // Delete the test database
  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog_ptr->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  txn_manager.CommitTransaction(txn);
}
}  // End test namespace
}  // End peloton namespace
