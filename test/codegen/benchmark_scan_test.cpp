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

#include "common/harness.h"

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "codegen/buffering_consumer.h"
#include "codegen/query_compiler.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/seq_scan_executor.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "logging/log_manager.h"
#include "planner/seq_scan_plan.h"
#include "storage/table_factory.h"
#include "type/types.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

struct Stats {
  codegen::QueryCompiler::CompileStats compile_stats{0.0, 0.0, 0.0};
  codegen::Query::RuntimeStats runtime_stats{0.0, 0.0, 0.0};
  double num_samples = 0.0;
  int32_t tuple_result_size = -1;

  void Merge(codegen::QueryCompiler::CompileStats &o_compile_stats,
             codegen::Query::RuntimeStats &o_runtime_stats,
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

  void PrintStats() {
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
};

class BenchmarkScanTest : public PelotonCodeGenTest {
 public:
  BenchmarkScanTest() : PelotonCodeGenTest() {
    // Load test table
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  uint32_t TestTableId() { return test_table1_id; }

  Stats RunCompiledExperiment(uint32_t num_runs = 5) {
      // Keep one copy of compile and runtime stats
      Stats stats;

      for (uint32_t i = 0; i < num_runs; i++) {
        planner::SeqScanPlan scan{&GetTestTable(TestTableId()), nullptr, {0, 1, 2}};

        // Do binding
        planner::BindingContext context;
        scan.PerformBinding(context);

        // We collect the results of the query into an in-memory buffer
        codegen::BufferingConsumer buffer{{0, 1, 2}, context};

        // COMPILE and execute
        codegen::Query::RuntimeStats runtime_stats;
        codegen::QueryCompiler::CompileStats compile_stats = CompileAndExecute(scan, buffer, reinterpret_cast<char*>(buffer.GetState()), &runtime_stats);

        stats.Merge(compile_stats, runtime_stats,
            buffer.GetOutputTuples().size());
      }

      stats.Finalize();
      return stats;
    }

    Stats RunInterpretedExperiment(uint32_t num_runs = 5) {
      // Keep one copy of compile and runtime stats
      Stats stats;

      for (uint32_t i = 0; i < num_runs; i++) {
        planner::SeqScanPlan scan{&GetTestTable(TestTableId()), nullptr, {0, 1, 2}};
        std::vector<std::vector<type::Value>> vals;

        codegen::QueryCompiler::CompileStats compile_stats;
        codegen::Query::RuntimeStats runtime_stats;

        auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
        auto txn = txn_manager.BeginTransaction();

        executor::ExecutorContext ctx{txn};
        executor::SeqScanExecutor executor{&scan, &ctx};

        executor.Init();

        Timer<std::ratio<1, 1000>> timer;
        timer.Start();
        while (executor.Execute()) {
          auto tile = executor.GetOutput();
          for (oid_t tuple_id : *tile) {
            const expression::ContainerTuple<executor::LogicalTile> tuple{
                tile, tuple_id};
            std::vector<type::Value> tv;
            for (oid_t col_id : scan.GetColumnIds()) {
              tv.push_back(tuple.GetValue(col_id));
            }
            vals.push_back(std::move(tv));
          }
        }
        timer.Stop();
        runtime_stats.plan_ms = timer.GetDuration();

        stats.Merge(compile_stats, runtime_stats, vals.size());

        txn_manager.CommitTransaction(txn);
      }

      stats.Finalize();
      return stats;
    }

 private:
  uint32_t num_rows_to_insert = 10000;
};

void PrintName(std::string test_name) {
  std::cerr << "NAME:\n===============\n" << test_name << std::endl;
}


TEST_F(BenchmarkScanTest, ScanTestWithCompilation) {
  PrintName("SCAN: COMPILATION");
  auto stats = RunCompiledExperiment();
  stats.PrintStats();
}

TEST_F(BenchmarkScanTest, ScanTestWithInterpretation) {
  PrintName("SCAN: INTERPRETATION");
  auto stats = RunInterpretedExperiment();
  stats.PrintStats();
}

//TEST_F(BenchmarkScanTest, PredicateComplexityTestWithCompilation) {
//  ScanComplexity complexities[] = { SIMPLE, MODERATE, COMPLEX };
//
//  PrintName("SCAN_COMPLEXITY: COMPILATION");
//  for (ScanComplexity complexity : complexities) {
//    TestConfig config;
//    config.layout = LAYOUT_TYPE_ROW;
//    config.selectivity = 0.5;
//    config.scan_complexity = complexity;
//    config.scale_factor = 50;
//
//    auto stats = RunCompiledExperiment(config);
//    PrintConfig(config);
//    PrintStats(stats);
//  }
//}
//
//TEST_F(BenchmarkScanTest, PredicateComplexityTestWithInterpretation) {
//  ScanComplexity complexities[] = { SIMPLE, MODERATE, COMPLEX };
//
//  PrintName("SCAN_COMPLEXITY: INTERPRETATION");
//  for (ScanComplexity complexity : complexities) {
//    TestConfig config;
//    config.layout = LAYOUT_TYPE_ROW;
//    config.selectivity = 0.5;
//    config.scan_complexity = complexity;
//    config.scale_factor = 50;
//
//    auto stats = RunInterpretedExperiment(config);
//    PrintConfig(config);
//    PrintStats(stats);
//  }
//}

}  // namespace test
}  // namespace peloton
