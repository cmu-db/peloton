//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_translator_test.cpp
//
// Identification: test/codegen/hash_join_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_compiler.h"
#include "codegen/runtime_functions_proxy.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/hash_join_executor.h"
#include "executor/seq_scan_executor.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"
#include "storage/table_factory.h"
#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "planner/seq_scan_plan.h"

#include "codegen/codegen_test_util.h"
#include "executor/executor_tests_util.h"

namespace peloton {
namespace test {

enum JoinComplexity { SIMPLE, MODERATE, COMPLEX };

struct TestConfig {
  LayoutType layout = LAYOUT_ROW;
  uint32_t column_count = 8;
  uint32_t tuples_per_tilegroup = 50000;
  uint32_t scale_factor = 20;
  uint32_t relation_id = 0;
  JoinComplexity complexity = SIMPLE;
};

struct Stats {
  codegen::QueryCompiler::CompileStats compile_stats{0.0, 0.0, 0.0};
  codegen::QueryStatement::RuntimeStats runtime_stats{0.0, 0.0, 0.0};
  double num_samples = 0.0;
  int32_t tuple_result_size = -1;

  void Merge(codegen::QueryCompiler::CompileStats &o_compile_stats,
             codegen::QueryStatement::RuntimeStats &o_runtime_stats,
             int32_t o_tuple_result_size) {
    compile_stats.ir_gen_ms += o_compile_stats.ir_gen_ms;
    compile_stats.jit_ms += o_compile_stats.jit_ms;
    compile_stats.setup_ms += o_compile_stats.setup_ms;

    runtime_stats.init_ms += o_runtime_stats.init_ms;
    runtime_stats.plan_ms += o_runtime_stats.plan_ms;
    runtime_stats.tear_down_ms += o_runtime_stats.tear_down_ms;

    if (tuple_result_size < 0) {
      tuple_result_size = o_tuple_result_size;
    } else if (tuple_result_size != o_tuple_result_size) {
      throw Exception{
          "ERROR: tuple result size should not"
          " vary for the same test!"};
    }

    num_samples++;
  }

  void Finalize() {
    compile_stats.ir_gen_ms /= num_samples;
    compile_stats.jit_ms /= num_samples;
    compile_stats.setup_ms /= num_samples;

    runtime_stats.init_ms /= num_samples;
    runtime_stats.plan_ms /= num_samples;
    runtime_stats.tear_down_ms /= num_samples;
  }
};

class BenchmarkJoinTest : public PelotonTest {
 public:
  typedef std::unique_ptr<const expression::AbstractExpression> AbstractExprPtr;

  ~BenchmarkJoinTest() { DropDatabase(); }

  void CreateDatabase() {
    assert(database == nullptr);
    database = new storage::Database(0);
    catalog::Manager::GetInstance().AddDatabase(database);
  }

  void DropDatabase() {
    if (database != nullptr) {
      catalog::Manager::GetInstance().DropDatabaseWithOid(database->GetOid());
      database = nullptr;
    }
  }

  void CreateTable(TestConfig &config) {
    // First set the layout of the table before loading
    peloton_layout_mode = config.layout;

    const bool is_inlined = true;

    // Create schema first
    std::vector<catalog::Column> columns;

    for (oid_t col_itr = 0; col_itr < config.column_count; col_itr++) {
      auto column =
          catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "COL_" + std::to_string(col_itr), is_inlined);

      columns.push_back(column);
    }

    catalog::Schema *table_schema = new catalog::Schema(columns);
    std::string table_name("JOIN_TABLE_" + std::to_string(config.relation_id));

    /////////////////////////////////////////////////////////
    // Create table.
    /////////////////////////////////////////////////////////

    bool own_schema = true;
    bool adapt_table = true;
    auto *table = storage::TableFactory::GetDataTable(
        GetDatabase().GetOid(), config.relation_id, table_schema, table_name,
        config.tuples_per_tilegroup, own_schema, adapt_table);

    /////////////////////////////////////////////////////////
    // Add table to database.
    /////////////////////////////////////////////////////////

    GetDatabase().AddTable(table);
  }

  void LoadTable(TestConfig &config) {
    const int tuple_count = config.scale_factor * config.tuples_per_tilegroup;

    auto table_schema = GetTable(config.relation_id).GetSchema();

    /////////////////////////////////////////////////////////
    // Load in the data
    /////////////////////////////////////////////////////////

    // Insert tuples into tile_group.
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    const bool allocate = true;
    auto txn = txn_manager.BeginTransaction();
    std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));

    int rowid;
    for (rowid = 0; rowid < tuple_count; rowid++) {
      int populate_value = rowid;

      storage::Tuple tuple(table_schema, allocate);

      for (oid_t col_itr = 0; col_itr < config.column_count; col_itr++) {
        auto value = ValueFactory::GetIntegerValue(populate_value);
        tuple.SetValue(col_itr, value, pool.get());
      }

      ItemPointer tuple_slot_id =
          GetTable(config.relation_id).InsertTuple(&tuple);
      assert(tuple_slot_id.block != INVALID_OID);
      assert(tuple_slot_id.offset != INVALID_OID);
      txn->RecordInsert(tuple_slot_id);
    }

    txn_manager.CommitTransaction();
  }

  void CreateAndLoadTable(TestConfig &config) {
    CreateTable(config);
    LoadTable(config);
  }

  storage::Database &GetDatabase() const { return *database; }

  storage::DataTable &GetLeftTable() const { return GetTable(0); }

  storage::DataTable &GetRightTable() const { return GetTable(1); }

  storage::DataTable &GetTable(oid_t relation_id) const {
    return *GetDatabase().GetTableWithOid(relation_id);
  }

  AbstractExprPtr ConstructSimplePredicate() {
    // Construct join predicate:
    //   left_table.a = right_table.a
    auto *left_a =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    auto *right_a =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    AbstractExprPtr left_a_eq_right_a{
        new expression::ComparisonExpression<expression::CmpEq>(
            EXPRESSION_TYPE_COMPARE_EQUAL, left_a, right_a)};
    return left_a_eq_right_a;
  }

  AbstractExprPtr ConstructModeratePredicate() {
    // Construct join predicate:
    //   left_table.a = right_table.a AND left_table.b = right_table.b + 100

    // left_table.a = right_table.a
    auto *left_a =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    auto *right_a =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    auto *left_a_eq_right_a =
        new expression::ComparisonExpression<expression::CmpEq>(
            EXPRESSION_TYPE_COMPARE_EQUAL, left_a, right_a);

    // right_table.b + 100
    auto *right_b =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
    auto *const_100_exp = new expression::ConstantValueExpression(
        ValueFactory::GetIntegerValue(100));
    auto *right_b_plus_100 =
        new expression::OperatorExpression<expression::OpPlus>(
            EXPRESSION_TYPE_OPERATOR_PLUS, right_b, const_100_exp);

    // left_table.b = right_table.b + 100
    auto *left_b =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
    auto *left_b_eq_right_b_plus_100 =
        new expression::ComparisonExpression<expression::CmpEq>(
            EXPRESSION_TYPE_COMPARE_EQUAL, left_b, right_b_plus_100);

    // left_table.a = right_table.a AND left_table.b = right_table.b + 100
    AbstractExprPtr conj_exp{
        new expression::ConjunctionExpression<expression::ConjunctionAnd>(
            EXPRESSION_TYPE_CONJUNCTION_AND, left_a_eq_right_a,
            left_b_eq_right_b_plus_100)};
    return conj_exp;
  }

  AbstractExprPtr ConstructComplexPredicate() {
    // Construct join predicate:
    //   left_table.a = right_table.a AND left_table.b = right_table.b + 100
    //       AND left_table.c = right_table.a * 1000

    // left_table.a = right_table.a
    auto *left_a =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    auto *right_a1 =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    auto *left_a_eq_right_a =
        new expression::ComparisonExpression<expression::CmpEq>(
            EXPRESSION_TYPE_COMPARE_EQUAL, left_a, right_a1);

    // right_table.b + 100
    auto *right_b =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
    auto *const_100_exp = new expression::ConstantValueExpression(
        ValueFactory::GetIntegerValue(100));
    auto *right_b_plus_100 =
        new expression::OperatorExpression<expression::OpPlus>(
            EXPRESSION_TYPE_OPERATOR_PLUS, right_b, const_100_exp);

    // left_table.b = right_table.b + 100
    auto *left_b =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
    auto *left_b_eq_right_b_plus_100 =
        new expression::ComparisonExpression<expression::CmpEq>(
            EXPRESSION_TYPE_COMPARE_EQUAL, left_b, right_b_plus_100);

    // left_table.a = right_table.a AND left_table.b = right_table.b + 100
    auto *conj_exp_1 =
        new expression::ConjunctionExpression<expression::ConjunctionAnd>(
            EXPRESSION_TYPE_CONJUNCTION_AND, left_a_eq_right_a,
            left_b_eq_right_b_plus_100);

    // right_table.a * 1000
    auto *right_a2 =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    auto *const_1000_exp = new expression::ConstantValueExpression(
        ValueFactory::GetIntegerValue(1000));
    auto *right_a_times_1000 =
        new expression::OperatorExpression<expression::OpMultiply>(
            EXPRESSION_TYPE_OPERATOR_MULTIPLY, right_a2, const_1000_exp);

    // left_table.c = right_table.a * 1000
    auto *left_c =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 2);
    auto *left_c_eq_right_a_mul_1000 =
        new expression::ComparisonExpression<expression::CmpEq>(
            EXPRESSION_TYPE_COMPARE_EQUAL, left_c, right_a_times_1000);

    // left_table.a = right_table.a AND left_table.b = right_table.b + 100
    //     AND left_table.c = right_table.a * 1000
    AbstractExprPtr conj_exp_2{
        new expression::ConjunctionExpression<expression::ConjunctionAnd>(
            EXPRESSION_TYPE_CONJUNCTION_AND, conj_exp_1,
            left_c_eq_right_a_mul_1000)};
    return conj_exp_2;
  }

  std::unique_ptr<planner::HashJoinPlan> ConstructJoinPlan(
      JoinComplexity complexity) {
    AbstractExprPtr predicate = nullptr;
    switch (complexity) {
      case JoinComplexity::SIMPLE:
        predicate = ConstructSimplePredicate();
        break;
      case JoinComplexity::MODERATE:
        predicate = ConstructModeratePredicate();
        break;
      case JoinComplexity::COMPLEX:
        predicate = ConstructModeratePredicate();
        break;
      default: { throw Exception{"nope"}; }
    }

    // Projection:  [left_table.a, right_table.a, left_table.b, right_table.c]
    planner::ProjectInfo::DirectMap dm1 =
        std::make_pair(0, std::make_pair(0, 0));
    planner::ProjectInfo::DirectMap dm2 =
        std::make_pair(1, std::make_pair(1, 0));
    planner::ProjectInfo::DirectMap dm3 =
        std::make_pair(2, std::make_pair(0, 1));
    planner::ProjectInfo::DirectMap dm4 =
        std::make_pair(3, std::make_pair(1, 2));
    planner::ProjectInfo::DirectMapList direct_map_list = {dm1, dm2, dm3, dm4};
    std::unique_ptr<planner::ProjectInfo> projection{new planner::ProjectInfo(
        planner::ProjectInfo::TargetList{}, std::move(direct_map_list))};

    // Output schema
    auto schema = std::shared_ptr<const catalog::Schema>(
        new catalog::Schema({ExecutorTestsUtil::GetColumnInfo(0),
                             ExecutorTestsUtil::GetColumnInfo(0),
                             ExecutorTestsUtil::GetColumnInfo(1),
                             ExecutorTestsUtil::GetColumnInfo(2)}));

    // Left and right hash keys
    std::vector<AbstractExprPtr> left_hash_keys;
    left_hash_keys.emplace_back(
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0));

    std::vector<AbstractExprPtr> right_hash_keys;
    right_hash_keys.emplace_back(
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 1, 0));

    std::vector<AbstractExprPtr> hash_keys;
    hash_keys.emplace_back(
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 1, 0));

    // Finally, the fucking join node
    std::unique_ptr<planner::HashJoinPlan> hj_plan{new planner::HashJoinPlan(
        JOIN_TYPE_INNER, std::move(predicate), std::move(projection), schema,
        left_hash_keys, right_hash_keys)};
    std::unique_ptr<planner::HashPlan> hash_plan{
        new planner::HashPlan(hash_keys)};

    std::unique_ptr<planner::AbstractPlan> left_scan{
        new planner::SeqScanPlan(&GetLeftTable(), nullptr, {0, 1, 2})};
    std::unique_ptr<planner::AbstractPlan> right_scan{
        new planner::SeqScanPlan(&GetRightTable(), nullptr, {0, 1, 2})};

    hash_plan->AddChild(std::move(right_scan));
    hj_plan->AddChild(std::move(left_scan));
    hj_plan->AddChild(std::move(hash_plan));

    return hj_plan;
  }

  Stats RunCompiledExperiment(TestConfig &left_table_config,
                              TestConfig &right_table_config,
                              uint32_t num_runs = 5) {
    // Keep one copy of compile and runtime stats
    Stats stats;

    //
    for (uint32_t i = 0; i < num_runs; i++) {
      // Create fresh database, tables and loaded data
      CreateDatabase();
      CreateAndLoadTable(left_table_config);
      CreateAndLoadTable(right_table_config);

      auto join_plan = ConstructJoinPlan(left_table_config.complexity);

      // Do binding
      planner::BindingContext context;
      join_plan->PerformBinding(context);

      // We collect the results of the query into an in-memory buffer
      BufferingConsumer buffer{{0, 1, 2}, context};

      // COMPILE and execute
      codegen::QueryCompiler compiler;
      codegen::QueryCompiler::CompileStats compile_stats;
      auto query_statement =
          compiler.Compile(*join_plan, buffer, &compile_stats);

      codegen::QueryStatement::RuntimeStats runtime_stats;
      query_statement->Execute(*catalog::Catalog::GetInstance(),
                               reinterpret_cast<char *>(buffer.GetState()),
                               &runtime_stats);

      stats.Merge(compile_stats, runtime_stats,
                  buffer.GetOutputTuples().size());

      // Cleanup
      DropDatabase();
    }

    stats.Finalize();
    return stats;
  }

  Stats RunInterpretedExperiment(TestConfig &left_table_config,
      TestConfig &right_table_config, uint32_t num_runs = 5) {
    // Keep one copy of compile and runtime stats
    Stats stats;

    //
    for (uint32_t i = 0; i < num_runs; i++) {
      codegen::QueryCompiler::CompileStats compile_stats{0.0,0.0,0.0};
      codegen::QueryStatement::RuntimeStats runtime_stats{0.0,0.0,0.0};

      // Create fresh database, table and loaded data
      CreateDatabase();
      CreateAndLoadTable(left_table_config);
      CreateAndLoadTable(right_table_config);

      auto join_plan = ConstructJoinPlan(left_table_config.complexity);

      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = peloton::concurrency::current_txn;
      // This happens for single statement queries in PG
      if (txn == nullptr) {
        txn = txn_manager.BeginTransaction();
      }

      executor::ExecutorContext ctx{txn};
      executor::HashJoinExecutor hj_exec{join_plan.get(), &ctx};

      executor::SeqScanExecutor left_exec{join_plan->GetChild(0), &ctx};
      executor::HashExecutor hash_exec{join_plan->GetChild(1), &ctx};
      executor::SeqScanExecutor right_exec{join_plan->GetChild(1)->GetChild(0), &ctx};


      hj_exec.AddChild(&left_exec);
      hj_exec.AddChild(&hash_exec);
      hash_exec.AddChild(&right_exec);

      common::StopWatch sw{true};
      hj_exec.Init();
      runtime_stats.init_ms = sw.elapsedMicros(true);

      // Run the hash_join_executor
      std::vector<std::vector<Value>> vals;
      while (hj_exec.Execute()) {
        auto tile = hj_exec.GetOutput();
        for (oid_t tuple_id : *tile) {
          const expression::ContainerTuple<executor::LogicalTile> tuple{
              tile, tuple_id};
          std::vector<Value> tv;
          for (oid_t col_id : {0,1,2}) {
            tv.push_back(tuple.GetValue(col_id));
          }
          vals.push_back(std::move(tv));
        }
      }
      runtime_stats.plan_ms = sw.elapsedMillis(true);

      stats.Merge(compile_stats, runtime_stats, vals.size());

      // Cleanup
      DropDatabase();
    }

    stats.Finalize();
    return stats;
  }

  void PrintName(std::string test_name) {
    std::cerr << "NAME:\n===============\n" << test_name << std::endl;
  }

  void PrintConfigs(TestConfig &left_table_config,
                    TestConfig &right_table_config) {
    TestConfig configs[] = {left_table_config, right_table_config};
    fprintf(stderr, "CONFIGURATION:\n===============\n");
    for (TestConfig &config : configs) {
      fprintf(stderr,
              "Table ID: %d, Layout: %d, # Cols: %d, # Tuples/tilegroup: %d, "
              "Scale factor: %d, Join complexity: %d\n",
              config.relation_id, config.layout, config.column_count,
              config.tuples_per_tilegroup, config.scale_factor,
              config.complexity);
    }
  }

  void PrintStats(Stats &stats) {
    auto &compile_stats = stats.compile_stats;
    auto &runtime_stats = stats.runtime_stats;
    auto &tuple_result_size = stats.tuple_result_size;
    fprintf(
        stderr,
        "Setup time: %.2f ms, IR Gen time: %.2f ms, Compile time: %.2f ms\n",
        compile_stats.setup_ms, compile_stats.ir_gen_ms, compile_stats.jit_ms);
    fprintf(stderr,
            "Initialization time: %.2f ms, execution time: %.2f ms, Tear down "
            "time: %.2f ms\n",
            runtime_stats.init_ms, runtime_stats.plan_ms,
            runtime_stats.tear_down_ms);
    fprintf(stderr, "Tuple result size: %d\n", tuple_result_size);
  }

 private:
  storage::Database *database = nullptr;
};

TEST_F(BenchmarkJoinTest, RowLayoutWithCompilationTest) {
  JoinComplexity complexities[] = {SIMPLE, MODERATE, COMPLEX};

  PrintName("JOIN_COMPLEXITY: COMPILATION");
  for (JoinComplexity complexity : complexities) {
    TestConfig left_table_config, right_table_config;
    left_table_config.layout = LAYOUT_COLUMN;
    left_table_config.complexity = complexity;
    left_table_config.scale_factor /= 5;

    right_table_config.layout = LAYOUT_ROW;
    right_table_config.complexity = complexity;
    right_table_config.relation_id = 1;

    auto stats = RunCompiledExperiment(left_table_config, right_table_config, 1);
    PrintConfigs(left_table_config, right_table_config);
    PrintStats(stats);
  }
}

TEST_F(BenchmarkJoinTest, RowLayoutWithInterpretationTest) {
  JoinComplexity complexities[] = {SIMPLE, MODERATE, COMPLEX};

  PrintName("JOIN_COMPLEXITY: INTERPRETATION");
  for (JoinComplexity complexity : complexities) {
    TestConfig left_table_config, right_table_config;
    left_table_config.layout = LAYOUT_ROW;
    left_table_config.complexity = complexity;
    right_table_config.layout = LAYOUT_COLUMN;
    right_table_config.complexity = complexity;
    right_table_config.relation_id = 1;
    right_table_config.scale_factor /= 5;

    auto stats = RunInterpretedExperiment(left_table_config, right_table_config, 1);
    PrintConfigs(left_table_config, right_table_config);
    PrintStats(stats);
  }
}

}  // namespace test
}  // namespace peloton
