//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator_test.cpp
//
// Identification: test/codegen/index_scan_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/storage_manager.h"
#include "catalog/catalog.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"
#include "planner/index_scan_plan.h"
#include "planner/seq_scan_plan.h"
#include "index/index_factory.h"
#include <vector>
#include <set>
#include <algorithm>

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class IndexScanTranslatorTest : public PelotonCodeGenTest {
  std::string table_name_ = "table_with_index";

 public:
  IndexScanTranslatorTest() : PelotonCodeGenTest(), num_rows_to_insert_(64) {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert_);

    CreateAndLoadTableWithIndex();
  }

  void CreateAndLoadTableWithIndex() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();
    auto *catalog = catalog::Catalog::GetInstance();

    const bool is_inlined = true;
    std::vector<catalog::Column> cols = {
      {type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER), "COL_A", is_inlined},
      {type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL), "COL_B", is_inlined},
      {type::TypeId::TIMESTAMP, type::Type::GetTypeSize(type::TypeId::TIMESTAMP), "COL_C", is_inlined},
      {type::TypeId::VARCHAR, 25, "COL_D", !is_inlined}
    };
    std::unique_ptr<catalog::Schema> schema{new catalog::Schema(cols)};

    // Insert table in catalog
    catalog->CreateTable(test_db_name, table_name_, std::move(schema),
                         txn);
    txn_manager.CommitTransaction(txn);

    auto &table = GetTableWithIndex();

    // add index
    // This holds column ID in the underlying table that are being indexed
    std::vector<oid_t> key_attrs;

    // This holds schema of the underlying table, which stays all the same
    // for all indices on the same underlying table
    auto tuple_schema = table.GetSchema();

    // This points to the schmea of only columns indiced by the index
    // This is basically selecting tuple_schema() with key_attrs as index
    // but the order inside tuple schema is preserved - the order of schema
    // inside key_schema is not the order of real key
    catalog::Schema *key_schema;

    // This will be created for each index on the table
    // and the metadata is passed as part of the index construction paratemter
    // list
    index::IndexMetadata *index_metadata;

    /////////////////////////////////////////////////////////////////
    // Add index on column 0
    /////////////////////////////////////////////////////////////////
    key_attrs = {0};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);

    // This is not redundant
    // since the key schema always follows the ordering of the base table
    // schema, we need real ordering of the key columns
    key_schema->SetIndexedColumns(key_attrs);

    index_metadata = new index::IndexMetadata(
      "bwtree_index", 123, INVALID_OID, INVALID_OID, IndexType::BWTREE,
      IndexConstraintType::PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
      false);

    std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_metadata));

    table.AddIndex(pkey_index);

    // populate the table
    txn = txn_manager.BeginTransaction();
    std::srand(std::time(nullptr));
    const bool allocate = true;
    auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
    std::set<int> key_set;
    for (uint32_t rowid = 0; rowid < test_table_size_; rowid++) {
      storage::Tuple tuple(tuple_schema, allocate);

      int key = std::rand();
      std::set<int>::iterator it = key_set.find(key);
      while (it != key_set.end()) {
        key = std::rand();
      }
      key_set.insert(key);
      keys_.push_back(key);

      tuple.SetValue(0, type::ValueFactory::GetIntegerValue(key), testing_pool);
      tuple.SetValue(1, type::ValueFactory::GetDecimalValue((double)std::rand() / 10000.0), testing_pool);
      tuple.SetValue(2, type::ValueFactory::GetTimestampValue((int64_t)std::rand()), testing_pool);
      tuple.SetValue(3, type::ValueFactory::GetVarcharValue(std::to_string(std::rand())), testing_pool);

      ItemPointer *index_entry_ptr = nullptr;
      ItemPointer tuple_slot_id =
        table.InsertTuple(&tuple, txn, &index_entry_ptr);

      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
    }

    txn_manager.CommitTransaction(txn);

    // sort the keys
    std::sort(keys_.begin(), keys_.end());
  }

  oid_t TestTableId() { return test_table_oids[0]; }

  storage::DataTable &GetTableWithIndex() const {
    return *GetDatabase().GetTableWithName(table_name_);
  }

  uint32_t GetTestTableSize() { return test_table_size_; }

  int GetKey(uint32_t idx) {
    PL_ASSERT(idx < test_table_size_);
    return keys_[idx];
  }

 private:
  uint32_t num_rows_to_insert_ = 64;
  uint32_t test_table_size_ = 1000;
  std::vector<int> keys_;
};

TEST_F(IndexScanTranslatorTest, IndexPointQuery) {
  //
  // SELECT a, b, c, d FROM table where a = x;
  //

  auto &data_table = GetTableWithIndex();

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 2, 3});

  int key = GetKey(std::rand() % GetTestTableSize());
  //===--------------------------------------------------------------------===//
  // ATTR 0 == key
  //===--------------------------------------------------------------------===//
  auto index = data_table.GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(0);
  expr_types.push_back(
    ExpressionType::COMPARE_EQUAL);
  values.push_back(type::ValueFactory::GetIntegerValue(key).Copy());

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
    index, key_column_ids, expr_types, values, runtime_keys);

  expression::AbstractExpression *predicate = nullptr;

  // Create plan node.
  planner::IndexScanPlan scan(&data_table, predicate, column_ids, index_scan_desc);

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char *>(buffer.GetState()));

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());
}


TEST_F(IndexScanTranslatorTest, IndexRangeScan) {
  //
  // SELECT a, b, c, d FROM table where a = x;
  //

  auto &data_table = GetTableWithIndex();

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 2, 3});

  int key1_idx = std::rand() % (GetTestTableSize() / 2);
  int key2_idx = std::rand() % (GetTestTableSize() / 2) + key1_idx;

  int key1 = GetKey(key1_idx);
  int key2 = GetKey(key2_idx);
  //===--------------------------------------------------------------------===//
  // ATTR 0 >= key1 and ATTR 0 <= key2
  //===--------------------------------------------------------------------===//
  auto index = data_table.GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(0);
  expr_types.push_back(
    ExpressionType::COMPARE_GREATERTHANOREQUALTO);
  values.push_back(type::ValueFactory::GetIntegerValue(key1).Copy());

  key_column_ids.push_back(0);
  expr_types.push_back(
    ExpressionType::COMPARE_LESSTHANOREQUALTO);
  values.push_back(type::ValueFactory::GetIntegerValue(key2).Copy());

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
    index, key_column_ids, expr_types, values, runtime_keys);

  expression::AbstractExpression *predicate = nullptr;

  // Create plan node.
  planner::IndexScanPlan scan(&data_table, predicate, column_ids, index_scan_desc);

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char *>(buffer.GetState()));

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(key2_idx - key1_idx + 1, results.size());
}

TEST_F(IndexScanTranslatorTest, IndexFullScan) {
  //
  // SELECT a, b, c, d FROM table;
  //

  auto &data_table = GetTableWithIndex();

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 2, 3});

  auto index = data_table.GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
    index, key_column_ids, expr_types, values, runtime_keys);

  expression::AbstractExpression *predicate = nullptr;

  // Create plan node.
  planner::IndexScanPlan scan(&data_table, predicate, column_ids, index_scan_desc);

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer, reinterpret_cast<char *>(buffer.GetState()));

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(GetTestTableSize(), results.size());
}

}
}