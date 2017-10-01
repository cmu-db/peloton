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

#include "concurrency/transaction_manager_factory.h"
#include "sql/testing_sql_util.h"
#include "optimizer/cost_and_stats_calculator.h"
#include "catalog/catalog.h"
#include "optimizer/column_manager.h"
#include "optimizer/properties.h"
#include "optimizer/stats/table_stats.h"
#include "type/types.h"

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
	for (int i = 1; i <= N_ROW; i++) {
		std::stringstream ss;
		ss << "INSERT INTO test VALUES (" << i << ", 'name', 1.1);";
		TestingSQLUtil::ExecuteSQLQuery(ss.str());
	}
}

TEST_F(CostAndStatsCalculatorTests, SeqScanTest) {
	LOG_DEBUG("start test");
	auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
	auto txn = txn_manager.BeginTransaction();
	catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
	txn_manager.CommitTransaction(txn);
	// create table with name test
	CreateAndLoadTable();
	LOG_DEBUG("create database successed");

		// Collect stats
	TestingSQLUtil::ExecuteSQLQuery("ANALYZE test");


	txn = txn_manager.BeginTransaction();
	auto catalog = catalog::Catalog::GetInstance();
	auto table_ = catalog->GetTableWithName(DEFAULT_DB_NAME, "test", txn);
	Operator op = PhysicalSeqScan::make(table_, "", false);
	ColumnManager manager;
	CostAndStatsCalculator calculator(manager);


  std::vector<std::shared_ptr<expression::AbstractExpression>> cols;

  auto tv_expr = std::make_shared<expression::TupleValueExpression>("id");
  tv_expr->SetValueIdx(0, 0);

  cols.push_back(tv_expr);
  PropertySet *set = new PropertySet;
  set->AddProperty(std::shared_ptr<PropertyColumns>(new PropertyColumns(cols)));

  calculator.output_properties_ = set;
	op.Accept(dynamic_cast<OperatorVisitor*>(&calculator));
	txn_manager.CommitTransaction(txn);
	EXPECT_EQ(calculator.output_cost_, 1);
	LOG_DEBUG("output stat num row: %zd\n", ((TableStats*)calculator.output_stats_.release())->num_rows);


	// Free the database
	txn = txn_manager.BeginTransaction();
	catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
	txn_manager.CommitTransaction(txn);
}

}
}