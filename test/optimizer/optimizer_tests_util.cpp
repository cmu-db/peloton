//
// Created by hzxa2 on 1/30/2017.
//

#include "planner/create_plan.h"
#include "parser/parser.h"
#include "optimizer/optimizer_tests_util.h"
#include "tcop/tcop.h"

namespace peloton {
namespace test {

std::random_device rd;
std::mt19937 rng(rd());
tcop::TrafficCop OptimizerTestsUtil::traffic_cop_;

void OptimizerTestsUtil::CreateDatabase(const std::string name) {
  LOG_INFO("Create Database %s", name.c_str());
  catalog::Catalog::GetInstance()->CreateDatabase(name, nullptr);
}

void OptimizerTestsUtil::CreateTable(const std::string stmt) {
  auto& parser = parser::Parser::GetInstance();
  auto parsed_stmt = parser.BuildParseTree(stmt);
  auto create_table_plan = new planner::CreatePlan(
      (parser::CreateStatement*)(parsed_stmt->GetStatement(0)));

  LOG_INFO("Create table: %s", stmt.c_str());
  std::vector<type::Value> params;
  std::vector<StatementResult> result;
  auto tuple_descriptor = std::move(
      traffic_cop_.GenerateTupleDescriptor(parsed_stmt->GetStatement(0)));
  auto result_format = std::move(std::vector<int>(tuple_descriptor.size(), 0));
  traffic_cop_.ExecuteStatementPlan(create_table_plan, params, result, result_format);
}

int OptimizerTestsUtil::GenerateRandomInt(const int min, const int max) {
  std::uniform_int_distribution<> dist(min, max);

  int sample = dist(rng);
  return sample;
}

double OptimizerTestsUtil::GenerateRandomDouble(const double min, const double max) {
  std::uniform_real_distribution<> dist(min, max);

  double sample = dist(rng);
  return sample;
}

std::string OptimizerTestsUtil::GenerateRandomString(const int len) {
  std::string s(len, '0');
  static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
          "abcdefghijklmnopqrstuvwxyz";

  for (int i = 0; i < len; ++i) {
    s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
  }
  return s;
}

}
}