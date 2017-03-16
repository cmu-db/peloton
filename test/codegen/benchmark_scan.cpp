//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// benchmark_scan.cpp
//
// Identification: test/codegen/benchmark_scan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"

#include "backend/catalog/column.h"
#include "backend/catalog/schema.h"
#include "backend/codegen/query_compiler.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/conjunction_expression.h"
#include "backend/storage/table_factory.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

enum ScanComplexity { SIMPLE, MODERATE, COMPLEX, WTF };

struct TestConfig {
  LayoutType layout = LAYOUT_ROW;
  uint32_t column_count = 8;
  uint32_t tuples_per_tilegroup = 20000;
  uint32_t scale_factor = 20;
  ScanComplexity scan_complexity = MODERATE;
  double selectivity;
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
      throw Exception{"ERROR: tuple result size should not"
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

class BenchmarkScanTest : public PelotonTest {
 public:
  ~BenchmarkScanTest() { DropDatabase(); }

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
    std::string table_name("BENCHMARK_SCAN_TABLE");

    /////////////////////////////////////////////////////////
    // Create table.
    /////////////////////////////////////////////////////////

    bool own_schema = true;
    bool adapt_table = true;
    auto *table = storage::TableFactory::GetDataTable(
        GetDatabase().GetOid(), 0, table_schema, table_name,
        config.tuples_per_tilegroup, own_schema, adapt_table);

    /////////////////////////////////////////////////////////
    // Add table to database.
    /////////////////////////////////////////////////////////

    GetDatabase().AddTable(table);
  }

  void LoadTable(TestConfig &config) {
    const int tuple_count = config.scale_factor * config.tuples_per_tilegroup;

    auto table_schema = GetTable().GetSchema();

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

      ItemPointer tuple_slot_id = GetTable().InsertTuple(&tuple);
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

  storage::DataTable &GetTable() const {
    return *GetDatabase().GetTableWithOid(0);
  }

  expression::AbstractExpression *ConstructSimplePredicate(TestConfig &config) {
    // Setup the predicate of the form a >= ? such that the selectivity
    // is equal to that in the configuration
    double param = (1 - config.selectivity) *
                   (config.scale_factor * config.tuples_per_tilegroup);
    auto *a_col_exp =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    auto *const_exp = new expression::ConstantValueExpression(
        ValueFactory::GetIntegerValue(param));
    return new expression::ComparisonExpression<expression::CmpGte>(
        EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_exp);
  }

  expression::AbstractExpression *ConstructModeratePredicate(
      TestConfig &config) {
    // Setup the predicate a >= ? and b >= a such that the predicate has
    // the selectivity as configured for the experiment

    double param = (1 - config.selectivity) *
        (config.scale_factor * config.tuples_per_tilegroup);
    auto *a_col_exp =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    auto *const_exp = new expression::ConstantValueExpression(
        ValueFactory::GetIntegerValue(param));
    auto *a_gte_param = new expression::ComparisonExpression<expression::CmpGte>(
        EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_exp);

    auto *b_col_exp =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
    auto *a_col_exp2 =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    auto *b_gte_a = new expression::ComparisonExpression<expression::CmpGte>(
        EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, b_col_exp, a_col_exp2);

    return new expression::ConjunctionExpression<expression::ConjunctionAnd>(
        EXPRESSION_TYPE_CONJUNCTION_AND, a_gte_param, b_gte_a);
  }

  expression::AbstractExpression *ConstructComplexPredicate(
      __attribute((unused)) TestConfig &config) {
    // Setup the predicate a >= ? and b >= a and c >= b
    double param = (1 - config.selectivity) *
        (config.scale_factor * config.tuples_per_tilegroup);
    auto *a_col_exp =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    auto *const_exp = new expression::ConstantValueExpression(
        ValueFactory::GetIntegerValue(param));
    auto *a_gte_param = new expression::ComparisonExpression<expression::CmpGte>(
        EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_exp);

    auto *b_col_exp =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
    auto *a_col_exp2 =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
    auto *b_gte_a = new expression::ComparisonExpression<expression::CmpGte>(
        EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO, b_col_exp, a_col_exp2);

    auto *c_col_exp =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 2);
    auto *b_col_exp2 =
        new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 1);
    auto *c_gte_b = new expression::ComparisonExpression<expression::CmpLte>(
        EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO, c_col_exp, b_col_exp2);

    auto *b_gte_a_gte_param =
        new expression::ConjunctionExpression<expression::ConjunctionAnd>(
            EXPRESSION_TYPE_CONJUNCTION_AND, a_gte_param, b_gte_a);
    return new expression::ConjunctionExpression<expression::ConjunctionAnd>(
        EXPRESSION_TYPE_CONJUNCTION_AND, b_gte_a_gte_param, c_gte_b);
  }

  std::unique_ptr<planner::SeqScanPlan> ConstructScanPlan(TestConfig &config) {
    expression::AbstractExpression *predicate = nullptr;
    switch (config.scan_complexity) {
      case ScanComplexity::SIMPLE:
        predicate = ConstructSimplePredicate(config);
        break;
      case ScanComplexity::MODERATE:
        predicate = ConstructModeratePredicate(config);
        break;
      case ScanComplexity::COMPLEX:
        predicate = ConstructComplexPredicate(config);
        break;
      default: { throw Exception{"nope"}; }
    }

    // Setup the scan plan node
    return std::unique_ptr<planner::SeqScanPlan>{
        new planner::SeqScanPlan(&GetTable(), predicate, {0, 1, 2})};
  }

  Stats RunCompiledExperiment(TestConfig &config, uint32_t num_runs = 5) {
    // Keep one copy of compile and runtime stats
    Stats stats;

    //
    for (uint32_t i = 0; i < num_runs; i++) {
      // Create fresh database, table and loaded data
      CreateDatabase();
      CreateAndLoadTable(config);

      auto scan = ConstructScanPlan(config);

      // Do binding
      planner::BindingContext context;
      scan->PerformBinding(context);

      // We collect the results of the query into an in-memory buffer
      BufferingConsumer buffer{{0, 1, 2}, context};

      // COMPILE and execute
      codegen::QueryCompiler compiler;
      codegen::QueryCompiler::CompileStats compile_stats;
      auto query_statement = compiler.Compile(*scan, buffer, &compile_stats);

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

  Stats RunInterpretedExperiment(TestConfig &config, uint32_t num_runs = 5) {
    // Keep one copy of compile and runtime stats
    Stats stats;

    //
    for (uint32_t i = 0; i < num_runs; i++) {
      std::vector<std::vector<Value>> vals;

      codegen::QueryCompiler::CompileStats compile_stats;
      codegen::QueryStatement::RuntimeStats runtime_stats;

      // Create fresh database, table and loaded data
      CreateDatabase();
      CreateAndLoadTable(config);

      auto scan = ConstructScanPlan(config);

      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = peloton::concurrency::current_txn;
      // This happens for single statement queries in PG
      if (txn == nullptr) {
        txn = txn_manager.BeginTransaction();
      }

      executor::ExecutorContext ctx{txn};
      executor::SeqScanExecutor executor{scan.get(), &ctx};

      executor.Init();

      common::StopWatch sw{true};
      while (executor.Execute()) {
        auto tile = executor.GetOutput();
        for (oid_t tuple_id : *tile) {
          const expression::ContainerTuple<executor::LogicalTile> tuple{
              tile, tuple_id};
          std::vector<Value> tv;
          for (oid_t col_id : scan->GetColumnIds()) {
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

    // stats.Finalize();
    return stats;
  }

  void PrintName(std::string test_name) {
    std::cerr << "NAME:\n===============\n" << test_name << std::endl;
  }

  void PrintConfig(TestConfig &config) {
    fprintf(stderr, "CONFIGURATION:\n===============\n");
    fprintf(stderr,
            "Layout: %d, # Cols: %d, # Tuples/tilegroup: %d, Scale factor: %d, "
            "Scan complexity: %d, Selectivity: %.2f\n",
            config.layout, config.column_count, config.tuples_per_tilegroup,
            config.scale_factor, config.scan_complexity, config.selectivity);
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
    fprintf(stderr,
            "Tuple result size: %d\n",
            tuple_result_size);
  }

 private:
  storage::Database *database = nullptr;
};

TEST_F(BenchmarkScanTest, SelectivityTestWithCompilation) {
  double selectivities[] = {0.0, 0.25, 0.5, 0.75, 1.0};

  PrintName("SCAN_SELECTIVITY: COMPILATION");
  for (double selectivity : selectivities) {
    TestConfig config;
    config.layout = LAYOUT_ROW;
    config.selectivity = selectivity;
    config.scan_complexity = SIMPLE;
    config.scale_factor = 50;

    auto stats = RunCompiledExperiment(config);
    PrintConfig(config);
    PrintStats(stats);
  }
}

TEST_F(BenchmarkScanTest, SelectivityTestWithInterpretation) {
  double selectivities[] = {0.0, 0.25, 0.5, 0.75, 1.0};

  PrintName("SCAN_SELECTIVITY: INTERPRETATION");
  for (double selectivity : selectivities) {
    TestConfig config;
    config.layout = LAYOUT_ROW;
    config.selectivity = selectivity;
    config.scan_complexity = SIMPLE;
    config.scale_factor = 50;

    auto stats = RunInterpretedExperiment(config);
    PrintConfig(config);
    PrintStats(stats);
  }
}

TEST_F(BenchmarkScanTest, PredicateComplexityTestWithCompilation) {
  ScanComplexity complexities[] = { SIMPLE, MODERATE, COMPLEX };

  PrintName("SCAN_COMPLEXITY: COMPILATION");
  for (ScanComplexity complexity : complexities) {
    TestConfig config;
    config.layout = LAYOUT_ROW;
    config.selectivity = 0.5;
    config.scan_complexity = complexity;
    config.scale_factor = 50;

    auto stats = RunCompiledExperiment(config);
    PrintConfig(config);
    PrintStats(stats);
  }
}

TEST_F(BenchmarkScanTest, PredicateComplexityTestWithInterpretation) {
  ScanComplexity complexities[] = { SIMPLE, MODERATE, COMPLEX };

  PrintName("SCAN_COMPLEXITY: INTERPRETATION");
  for (ScanComplexity complexity : complexities) {
    TestConfig config;
    config.layout = LAYOUT_ROW;
    config.selectivity = 0.5;
    config.scan_complexity = complexity;
    config.scale_factor = 50;

    auto stats = RunInterpretedExperiment(config);
    PrintConfig(config);
    PrintStats(stats);
  }
}

}  // namespace test
}  // namespace peloton
