//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_test.cpp
//
// Identification: test/optimizer/cost_and_stats_calculator_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "common/harness.h"

#define private public

#include "catalog/catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/column_manager.h"
#include "optimizer/cost_and_stats_calculator.h"
#include "optimizer/properties.h"
#include "optimizer/stats/table_stats.h"
#include "sql/testing_sql_util.h"
#include "common/internal_types.h"

namespace peloton {
namespace test {

using namespace optimizer;

const int N_ROW = 100;
class CostAndStatsCalculatorTests : public PelotonTest {};

// tablename: test
// database name: DEFAULT_DB_NAME
void CreateAndLoadTable() {
  TestingSQLUtil::ExecuteSQLQuery(
      "CREATE TABLE test(id INT PRIMARY KEY, name VARCHAR, salary DECIMAL);");
  TestingSQLUtil::ExecuteSQLQuery("CREATE INDEX salary_index ON test(salary);");
  for (int i = 1; i <= N_ROW; i++) {
    std::stringstream ss;
    ss << "INSERT INTO test VALUES (" << i << ", 'name', " << i % 3 + 1 << ");";
    TestingSQLUtil::ExecuteSQLQuery(ss.str());
  }
}

TEST_F(CostAndStatsCalculatorTests, NoConditionSeqScanTest) {
  // auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // auto txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);
  // // create table with name test
  // CreateAndLoadTable();
  // LOG_DEBUG("create database succeed");
  //
  // // Collect stats
  // TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");
  //
  // txn = txn_manager.BeginTransaction();
  // auto catalog = catalog::Catalog::GetInstance();
  // auto table_ = catalog->GetTableWithName(DEFAULT_DB_NAME, "test", txn);
  // txn_manager.CommitTransaction(txn);
  // Operator op = PhysicalSeqScan::make(0, table_, "", nullptr, false);
  // CostAndStatsCalculator calculator;
  //
  // std::vector<std::shared_ptr<expression::AbstractExpression>> cols;
  //
  // auto tv_expr = std::shared_ptr<expression::AbstractExpression>(
  //     expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
  //                                                   2));
  //
  // cols.push_back(tv_expr);
  // PropertySet *set = new PropertySet;
  // set->AddProperty(std::shared_ptr<PropertyColumns>(new PropertyColumns(cols)));
  //
  // calculator.output_properties_ = set;
  // op.Accept(dynamic_cast<OperatorVisitor *>(&calculator));
  //
  // EXPECT_EQ(calculator.output_cost_, 1);
  // LOG_INFO("output stat num row: %zu\n",
  //          (std::dynamic_pointer_cast<TableStats>(calculator.output_stats_))
  //              ->num_rows);
  //
  // // Free the database
  // delete set;
  // txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);
}

TEST_F(CostAndStatsCalculatorTests, SingleConditionSeqScanTest) {
  // auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // auto txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);
  // // create table with name test
  // CreateAndLoadTable();
  // LOG_DEBUG("create database succeed");
  //
  // // Collect stats
  // TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");
  //
  // txn = txn_manager.BeginTransaction();
  // auto catalog = catalog::Catalog::GetInstance();
  // auto table_ = catalog->GetTableWithName(DEFAULT_DB_NAME, "test", txn);
  // txn_manager.CommitTransaction(txn);
  // auto expr1 = expression::ExpressionUtil::TupleValueFactory(
  //     type::TypeId::DECIMAL, 0, 2);
  // auto expr2 = expression::ExpressionUtil::ConstantValueFactory(
  //     type::ValueFactory::GetDecimalValue(1.0));
  // auto predicate = expression::ExpressionUtil::ComparisonFactory(
  //     ExpressionType::COMPARE_EQUAL, expr1, expr2);
  // Operator op = PhysicalSeqScan::make(
  //     0, table_, "", std::shared_ptr<expression::AbstractExpression>(predicate),
  //     false);
  // CostAndStatsCalculator calculator;
  //
  // std::vector<std::shared_ptr<expression::AbstractExpression>> cols;
  //
  // auto tv_expr = std::shared_ptr<expression::AbstractExpression>(
  //     expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
  //                                                   2));
  //
  // cols.push_back(tv_expr);
  // PropertySet *set = new PropertySet;
  // set->AddProperty(std::make_shared<PropertyColumns>(cols));
  //
  // calculator.output_properties_ = set;
  // op.Accept(dynamic_cast<OperatorVisitor *>(&calculator));
  //
  // EXPECT_EQ(calculator.output_cost_, 1);
  // LOG_INFO("output stat num row: %zu\n",
  //          (std::dynamic_pointer_cast<TableStats>(calculator.output_stats_))
  //              ->num_rows);
  //
  // // Free the database
  // delete set;
  // txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);
}

TEST_F(CostAndStatsCalculatorTests, SingleConditionIndexScanTest) {
  // auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // auto txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);
  // // create table with name test
  // CreateAndLoadTable();
  // LOG_DEBUG("create database succeed");
  //
  // // Collect stats
  // TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");
  //
  // txn = txn_manager.BeginTransaction();
  // auto catalog = catalog::Catalog::GetInstance();
  // auto table_ = catalog->GetTableWithName(DEFAULT_DB_NAME, "test", txn);
  // txn_manager.CommitTransaction(txn);
  // // test.salary = 1.0
  // auto expr1 = new expression::TupleValueExpression("id");
  // expr1->SetTupleValueExpressionParams(type::TypeId::INTEGER, 0, 0);
  // auto expr2 = expression::ExpressionUtil::ConstantValueFactory(
  //     type::ValueFactory::GetIntegerValue(30));
  // auto predicate = expression::ExpressionUtil::ComparisonFactory(
  //     ExpressionType::COMPARE_GREATERTHAN, expr1, expr2);
  // Operator op = PhysicalIndexScan::make(
  //     0, table_, "", std::shared_ptr<expression::AbstractExpression>(predicate),
  //     false);
  // CostAndStatsCalculator calculator;
  //
  // std::vector<std::shared_ptr<expression::AbstractExpression>> cols;
  //
  // auto tv_expr = std::shared_ptr<expression::AbstractExpression>(
  //     expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
  //                                                   2));
  //
  // cols.push_back(tv_expr);
  // PropertySet *set = new PropertySet;
  // set->AddProperty(std::make_shared<PropertyColumns>(cols));
  //
  // calculator.output_properties_ = set;
  // op.Accept(dynamic_cast<OperatorVisitor *>(&calculator));
  //
  // EXPECT_EQ(((int)(calculator.output_cost_ * 100 + .5) / 100.0), 0.04);
  // LOG_INFO("output stat num row: %zu\n",
  //          (std::dynamic_pointer_cast<TableStats>(calculator.output_stats_))
  //              ->num_rows);
  //
  // // Free the database
  // delete set;
  // txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);
}

TEST_F(CostAndStatsCalculatorTests, ConjunctionConditionSeqScanTest) {
  // auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // auto txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);
  // // create table with name test
  // CreateAndLoadTable();
  // LOG_DEBUG("create database succeed");
  //
  // // Collect stats
  // TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");
  //
  // txn = txn_manager.BeginTransaction();
  // auto catalog = catalog::Catalog::GetInstance();
  // auto table_ = catalog->GetTableWithName(DEFAULT_DB_NAME, "test", txn);
  // txn_manager.CommitTransaction(txn);
  // // test.id > 30
  // auto expr1 = expression::ExpressionUtil::TupleValueFactory(
  //     type::TypeId::INTEGER, 0, 0);
  // auto expr2 = expression::ExpressionUtil::ConstantValueFactory(
  //     type::ValueFactory::GetIntegerValue(30));
  // auto expr3 = expression::ExpressionUtil::ComparisonFactory(
  //     ExpressionType::COMPARE_GREATERTHAN, expr1, expr2);
  //
  // // test.salary = 1.0
  // auto expr4 = expression::ExpressionUtil::TupleValueFactory(
  //     type::TypeId::DECIMAL, 0, 2);
  // auto expr5 = expression::ExpressionUtil::ConstantValueFactory(
  //     type::ValueFactory::GetDecimalValue(1.0));
  // auto expr6 = expression::ExpressionUtil::ComparisonFactory(
  //     ExpressionType::COMPARE_EQUAL, expr4, expr5);
  //
  // // (test.id > 30) && (test.salary = 1.0)
  // auto predicate = expression::ExpressionUtil::ConjunctionFactory(
  //     ExpressionType::CONJUNCTION_AND, expr3, expr6);
  // Operator op = PhysicalSeqScan::make(
  //     0, table_, "", std::shared_ptr<expression::AbstractExpression>(predicate),
  //     false);
  // CostAndStatsCalculator calculator;
  //
  // std::vector<std::shared_ptr<expression::AbstractExpression>> cols;
  //
  // auto tv_expr = std::shared_ptr<expression::AbstractExpression>(
  //     expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
  //                                                   2));
  //
  // cols.push_back(tv_expr);
  // PropertySet *set = new PropertySet;
  // set->AddProperty(std::make_shared<PropertyColumns>(cols));
  //
  // calculator.output_properties_ = set;
  // op.Accept(dynamic_cast<OperatorVisitor *>(&calculator));
  // EXPECT_EQ(calculator.output_cost_, 1.0);
  // LOG_INFO("output stat num row: %zu\n",
  //          (std::dynamic_pointer_cast<TableStats>(calculator.output_stats_))
  //              ->num_rows);
  //
  // // Free the database
  // delete set;
  // txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);
}

TEST_F(CostAndStatsCalculatorTests, ConjunctionConditionIndexScanTest) {
  // LOG_DEBUG("start test");
  // auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // auto txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);
  // // create table with name test
  // CreateAndLoadTable();
  // LOG_DEBUG("create database succeed");
  //
  // // Collect stats
  // TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");
  //
  // txn = txn_manager.BeginTransaction();
  // auto catalog = catalog::Catalog::GetInstance();
  // auto table_ = catalog->GetTableWithName(DEFAULT_DB_NAME, "test", txn);
  // txn_manager.CommitTransaction(txn);
  // // test.id > 30
  // auto expr1 = new expression::TupleValueExpression("id");
  // expr1->SetTupleValueExpressionParams(type::TypeId::INTEGER, 0, 0);
  // auto expr2 = expression::ExpressionUtil::ConstantValueFactory(
  //     type::ValueFactory::GetIntegerValue(30));
  // auto expr3 = expression::ExpressionUtil::ComparisonFactory(
  //     ExpressionType::COMPARE_GREATERTHAN, expr1, expr2);
  //
  // // test.id <= 90
  // auto expr4 = new expression::TupleValueExpression("id");
  // expr4->SetTupleValueExpressionParams(type::TypeId::INTEGER, 0, 2);
  // auto expr5 = expression::ExpressionUtil::ConstantValueFactory(
  //     type::ValueFactory::GetIntegerValue(90));
  // auto expr6 = expression::ExpressionUtil::ComparisonFactory(
  //     ExpressionType::COMPARE_LESSTHANOREQUALTO, expr4, expr5);
  //
  // // test.salary = 1.0
  // auto expr7 = expression::ExpressionUtil::TupleValueFactory(
  //     type::TypeId::DECIMAL, 0, 2);
  // auto expr8 = expression::ExpressionUtil::ConstantValueFactory(
  //     type::ValueFactory::GetDecimalValue(1.0));
  // auto expr9 = expression::ExpressionUtil::ComparisonFactory(
  //     ExpressionType::COMPARE_EQUAL, expr7, expr8);
  //
  // // (test.id > 30) && (test.id <= 90)
  // auto expr10 = expression::ExpressionUtil::ConjunctionFactory(
  //     ExpressionType::CONJUNCTION_AND, expr3, expr6);
  //
  // // ((test.id > 30) && (test.id <= 90)) && (test.salary = 1.0)
  // auto predicate = expression::ExpressionUtil::ConjunctionFactory(
  //     ExpressionType::CONJUNCTION_AND, expr10, expr9);
  // Operator op = PhysicalIndexScan::make(
  //     0, table_, "", std::shared_ptr<expression::AbstractExpression>(predicate),
  //     false);
  // CostAndStatsCalculator calculator;
  //
  // std::vector<std::shared_ptr<expression::AbstractExpression>> cols;
  //
  // auto tv_expr = std::shared_ptr<expression::AbstractExpression>(
  //     expression::ExpressionUtil::TupleValueFactory(type::TypeId::DECIMAL, 0,
  //                                                   2));
  //
  // cols.push_back(tv_expr);
  // PropertySet *set = new PropertySet;
  // set->AddProperty(std::make_shared<PropertyColumns>(cols));
  //
  // calculator.output_properties_ = set;
  // op.Accept(dynamic_cast<OperatorVisitor *>(&calculator));
  // EXPECT_EQ(((int)(calculator.output_cost_ * 1000 + .5) / 1000.0), 0.119);
  // LOG_INFO("output stat num row: %zu\n",
  //          (std::dynamic_pointer_cast<TableStats>(calculator.output_stats_))
  //              ->num_rows);
  //
  // // Free the database
  // delete set;
  // txn = txn_manager.BeginTransaction();
  // catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
  // txn_manager.CommitTransaction(txn);
}
}
}
