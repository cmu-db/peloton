//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bloom_filter_test.cpp
//
// Identification: test/codegen/bloom_filter_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdlib>
#include <unordered_set>
#include <vector>

#include "codegen/bloom_filter_accessor.h"
#include "codegen/codegen.h"
#include "codegen/counting_consumer.h"
#include "codegen/function_builder.h"
#include "codegen/query_parameters.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/bloom_filter_proxy.h"
#include "codegen/testing_codegen_util.h"
#include "codegen/util/bloom_filter.h"
#include "common/timer.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "executor/plan_executor.h"
#include "optimizer/optimizer.h"
#include "planner/hash_join_plan.h"
#include "planner/seq_scan_plan.h"
#include "sql/testing_sql_util.h"

namespace peloton {
namespace test {

class BloomFilterCodegenTest : public PelotonTest {
 public:
  BloomFilterCodegenTest() {
    // Create test db
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
  }

  ~BloomFilterCodegenTest() {
    // Drop test db
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);
  }

  int UpDivide(int num1, int num2) { return (num1 + num2 - 1) / num2; }

  void InsertTuple(const std::vector<int> &vals, storage::DataTable *table,
                   concurrency::TransactionContext *txn);

  void CreateTable(std::string table_name, int tuple_size,
                   concurrency::TransactionContext *txn);

  double ExecuteJoin(std::string query, concurrency::TransactionContext *txn,
                     int num_iter, unsigned inner_table_cardinality,
                     bool enable_bloom_filter);

  const std::string table1_name = "test1";
  const std::string table2_name = "test2";
};

TEST_F(BloomFilterCodegenTest, FalsePositiveRateTest) {
  codegen::CodeContext code_context;
  codegen::CodeGen codegen(code_context);

  // Generate an array of distinct random numbers.
  // Insert the first half into the bloom filter and
  // use the second half to test the false positive rate
  const int size = 100000;
  std::unordered_set<int> number_set;
  while (number_set.size() != size) {
    number_set.insert(rand());
  }
  std::vector<int> numbers(number_set.begin(), number_set.end());
  codegen::BloomFilterAccessor bloom_filter_accessor;

  // Build the test function that has the following logic:
  // define @TestBloomFilter(BloomFilter* bloom_filter, i32* numbers, i32 size,
  //                         i32* false_positive_cnt) {
  //   // Insert the first half into the bloom filter
  //   for (i32 i = 0; i < size / 2; i++) {
  //      bloom_filter.Add(numbers[i]);
  //   }
  //   // Test the second half and measure false positive cnt
  //   for (i32 i = size / 2; i < size; i++) {
  //      if (bloom_filter.Contains) {
  //         *false_positive_cnt ++;
  //      }
  //   }
  // }
  codegen::FunctionBuilder func{
      code_context,
      "TestBloomFilter",
      codegen.VoidType(),
      {{"bloom_filter",
        codegen::BloomFilterProxy::GetType(codegen)->getPointerTo()},
       {"numbers", codegen.Int32Type()->getPointerTo()},
       {"size", codegen.Int32Type()},
       {"false_positive_cnt", codegen.Int32Type()->getPointerTo()}}};
  {
    llvm::Value *bloom_filter = func.GetArgumentByPosition(0);
    llvm::Value *number_array = func.GetArgumentByPosition(1);
    llvm::Value *size_val = func.GetArgumentByPosition(2);
    llvm::Value *false_positive_cnt = func.GetArgumentByPosition(3);
    llvm::Value *index = codegen.Const32(0);
    llvm::Value *half_size = codegen->CreateUDiv(size_val, codegen.Const32(2));
    llvm::Value *finish_cond = codegen->CreateICmpULT(index, half_size);

    // Loop that inserts the first half of array into the bloom filter
    codegen::lang::Loop insert_loop{codegen, finish_cond, {{"i", index}}};
    {
      index = insert_loop.GetLoopVar(0);

      // Get numbers[i]
      llvm::Value *number = codegen->CreateLoad(
          codegen->CreateInBoundsGEP(codegen.Int32Type(), number_array, index));
      codegen::Value number_val{
          codegen::type::Type(peloton::type::TypeId::INTEGER, false), number};
      // Insert numbers[i] into bloom filter
      bloom_filter_accessor.Add(codegen, bloom_filter, {number_val});

      index = codegen->CreateAdd(index, codegen.Const32(1));
      insert_loop.LoopEnd(codegen->CreateICmpULT(index, half_size), {index});
    }

    // Loop that test the false positive rate
    finish_cond = codegen->CreateICmpULT(half_size, size_val);
    codegen::lang::Loop test_loop{codegen, finish_cond, {{"i", half_size}}};
    {
      index = test_loop.GetLoopVar(0);

      // Get numbers[i]
      llvm::Value *number = codegen->CreateLoad(
          codegen->CreateInBoundsGEP(codegen.Int32Type(), number_array, index));
      codegen::Value number_val{
          codegen::type::Type(peloton::type::TypeId::INTEGER, false), number};

      // Test if numbers[i] is contained in bloom filter
      llvm::Value *contains =
          bloom_filter_accessor.Contains(codegen, bloom_filter, {number_val});
      codegen::lang::If if_contains{codegen, contains};
      {
        codegen->CreateStore(
            codegen->CreateAdd(codegen->CreateLoad(false_positive_cnt),
                               codegen.Const32(1)),
            false_positive_cnt);
      }
      if_contains.EndIf();

      index = codegen->CreateAdd(index, codegen.Const32(1));
      test_loop.LoopEnd(codegen->CreateICmpULT(index, size_val), {index});
    }

    func.ReturnAndFinish();
  }

  ASSERT_TRUE(code_context.Compile());

  typedef void (*ftype)(codegen::util::BloomFilter *bloom_filter, int *, int,
                        int *);
  ftype f = (ftype)code_context.GetRawFunctionPointer(func.GetFunction());

  codegen::util::BloomFilter bloom_filter;
  bloom_filter.Init(size / 2);
  int num_false_positive = 0;

  // Run the function
  f(&bloom_filter, &numbers[0], size, &num_false_positive);
  double actual_FPR = (double)num_false_positive / (size / 2);
  double expected_FPR = codegen::util::BloomFilter::kFalsePositiveRate;
  LOG_INFO("Expected FPR %f, Actula FPR: %f", expected_FPR, actual_FPR);

  // Difference should be within 10%
  EXPECT_LT(expected_FPR * 0.9, actual_FPR);
  EXPECT_LT(actual_FPR, expected_FPR * 1.1);

  bloom_filter.Destroy();
}

// Testing whether bloom filter can improve the performance of hash join
// when the hash table is bigger than L3 cache and selectivity is low
TEST_F(BloomFilterCodegenTest, PerformanceTest) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *catalog = catalog::Catalog::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  // Initialize tables. test1 is the inner table from which we build the
  // hash table. test2 is the outer table which will probe the hash table.
  const int table1_tuple_size = 4096;
  const int table2_tuple_size = 8;
  const int bigint_size = 8;
  CreateTable(table1_name, table1_tuple_size, txn);
  CreateTable(table2_name, table2_tuple_size, txn);

  // Note: This should be changed to the size of L3 cache on the running machine
  const int L3_cache_size = 60000;
  const int table1_target_size = L3_cache_size * 10;
  const double selectivity = 0.2;
  const int outer_to_inner_ratio = 5;

  // Load the test1 until its size is bigger than a certain ratio of L3 cache.
  int curr_size = 0;
  std::vector<int> numbers;
  std::unordered_set<int> number_set;
  auto *table1 = catalog->GetTableWithName(DEFAULT_DB_NAME, table1_name, txn);
  while (curr_size < table1_target_size) {
    // Find a unique random number
    int random;
    do {
      random = rand();
    } while (number_set.count(random) == 1);
    numbers.push_back(random);
    number_set.insert(random);

    // Insert tuple into the table
    std::vector<int> vals(UpDivide(table1_tuple_size, bigint_size), random);
    InsertTuple(vals, table1, txn);

    curr_size += table1_tuple_size;
  }

  LOG_INFO("Finish populating test1");

  // Load the inner table which contains twice tuples as the outer table
  auto *table2 = catalog->GetTableWithName(DEFAULT_DB_NAME, table2_name, txn);
  unsigned outer_table_cardinality = numbers.size() * outer_to_inner_ratio;
  for (unsigned i = 0; i < outer_table_cardinality; i++) {
    int number;
    if (rand() % 100 < selectivity * 100) {
      // Pick a random number from the inner table
      number = numbers[rand() % numbers.size()];
    } else {
      // Pick a random number that is not in inner table
      do {
        number = rand();
      } while (number_set.count(number) == 1);
    }
    std::vector<int> vals(UpDivide(table2_tuple_size, bigint_size), number);
    InsertTuple(vals, table2, txn);
  }

  LOG_INFO("Finish populating test2\n");

  // Build and execute the join plan
  int num_iter = 3;
  std::string query =
      "SELECT * FROM test1 as t1, test2 as t2 "
      "WHERE t1.c0 = t2.c0";

  // Execute plan with bloom filter disabled
  LOG_INFO("Executing without bloom filter");
  double runtime1 = ExecuteJoin(query, txn, num_iter, numbers.size(), false);

  // Execute plan with bloom filter enabled
  LOG_INFO("\n");
  LOG_INFO("Executing with bloom filter");
  double runtime2 = ExecuteJoin(query, txn, num_iter, numbers.size(), true);

  LOG_INFO("Avg With Bloom Filter Disabled: %f", runtime1);
  LOG_INFO("Avg With Bloom Filter Enabled: %f", runtime2);
  LOG_INFO("Ratio: %f", runtime2 / runtime1);

  txn_manager.CommitTransaction(txn);
}

double BloomFilterCodegenTest::ExecuteJoin(std::string query,
                                           concurrency::TransactionContext *txn,
                                           int num_iter,
                                           unsigned inner_table_cardinality,
                                           bool enable_bloom_filter) {
  std::unique_ptr<optimizer::AbstractOptimizer> optimizer(
      new optimizer::Optimizer());
  double total_runtime = 0;
  // Run hash join multiple times and calculate the average runtime
  for (int i = 0; i < num_iter; i++) {
    auto plan =
        TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
    assert(((planner::SeqScanPlan *)plan->GetChild(0))->GetTable()->GetName() ==
           table1_name);
    // Change the bloom filter flag and set the correct cardinality in the plan
    const_cast<planner::AbstractPlan *>(plan->GetChild(0))
        ->SetCardinality(inner_table_cardinality);
    dynamic_cast<planner::HashJoinPlan *>(plan.get())
        ->SetBloomFilterFlag(enable_bloom_filter);

    // Binding
    planner::BindingContext context;
    plan->PerformBinding(context);
    // Use simple CountConsumer since we don't care about the result
    codegen::CountingConsumer consumer;
    // Compile the query
    codegen::QueryCompiler compiler;
    codegen::Query::RuntimeStats stats;
    std::unique_ptr<executor::ExecutorContext> executor_context(
        new executor::ExecutorContext{txn});
    auto compiled_query = compiler.Compile(
        *plan, executor_context->GetParams().GetQueryParametersMap(), consumer);

    // Run
    PelotonCodeGenTest::ExecuteSync(*compiled_query,
                                    std::move(executor_context), consumer);

    LOG_INFO("Execution Time: %0.0f ms", stats.plan_ms);
    total_runtime += stats.plan_ms;
  }
  return total_runtime / num_iter;
}

// Create a table where all the columns are BIGINT and each tuple has desired
// tuple size
void BloomFilterCodegenTest::CreateTable(std::string table_name, int tuple_size,
                                         concurrency::TransactionContext *txn) {
  int curr_size = 0;
  size_t bigint_size = type::Type::GetTypeSize(type::TypeId::BIGINT);
  std::vector<catalog::Column> cols;
  while (curr_size < tuple_size) {
    cols.push_back(
        catalog::Column{type::TypeId::BIGINT, bigint_size,
                        "c" + std::to_string(curr_size / bigint_size), true});
    curr_size += bigint_size;
  }
  auto *catalog = catalog::Catalog::GetInstance();
  catalog->CreateTable(
      DEFAULT_DB_NAME, table_name,
      std::unique_ptr<catalog::Schema>(new catalog::Schema(cols)), txn);
}

// Insert a tuple to specific table
void BloomFilterCodegenTest::InsertTuple(const std::vector<int> &vals,
                                         storage::DataTable *table,
                                         concurrency::TransactionContext *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  storage::Tuple tuple{table->GetSchema(), true};
  for (unsigned i = 0; i < vals.size(); i++) {
    tuple.SetValue(i, type::ValueFactory::GetBigIntValue(vals[i]));
  }
  ItemPointer *index_entry_ptr = nullptr;
  auto tuple_slot_id = table->InsertTuple(&tuple, txn, &index_entry_ptr);
  PL_ASSERT(tuple_slot_id.block != INVALID_OID);
  PL_ASSERT(tuple_slot_id.offset != INVALID_OID);
  txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
}

}  // namespace test
}  // namespace peloton
