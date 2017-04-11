#include "common/harness.h"
#include "commands/trigger.h"
#include "parser/pg_trigger.h"
#include "parser/postgresparser.h"
#include "planner/create_plan.h"

namespace peloton {
namespace test {

class TriggerTests : public PelotonTest {};

TEST_F(TriggerTests, BasicTest) {
  // Create statement
  auto parser = parser::PostgresParser::GetInstance();

  std::string query1 =
      "CREATE TRIGGER check_update "
      "BEFORE UPDATE OF balance ON accounts "
      "FOR EACH ROW "
      "WHEN (OLD.balance <> NEW.balance) "
      "EXECUTE PROCEDURE check_account_update();";
  auto stmt_list1 = parser.BuildParseTree(query1).release();
  EXPECT_TRUE(stmt_list1->is_valid);
  EXPECT_EQ(StatementType::CREATE, stmt_list1->GetStatement(0)->GetType());
  auto create_trigger_stmt1 =
      static_cast<parser::CreateStatement *>(stmt_list1->GetStatement(0));

  // Create plans
  const planner::CreatePlan plan1(create_trigger_stmt1);

  commands::Trigger trigger1(plan1);
  EXPECT_EQ("check_update", trigger1.GetTriggerName());
  int16_t trigger_type1 = trigger1.GetTriggerType();
  EXPECT_TRUE(TRIGGER_FOR_ROW(trigger_type1));
  EXPECT_TRUE(TRIGGER_FOR_BEFORE(trigger_type1));
  EXPECT_TRUE(TRIGGER_FOR_UPDATE(trigger_type1));
  EXPECT_FALSE(TRIGGER_FOR_DELETE(trigger_type1));

  std::string query2 =
      "CREATE TRIGGER check_update_and_delete "
      "BEFORE UPDATE OF balance OR DELETE ON accounts "
      "FOR EACH ROW "
      "WHEN (OLD.balance <> NEW.balance) "
      "EXECUTE PROCEDURE check_account_update();";
  auto stmt_list2 = parser.BuildParseTree(query2).release();
  EXPECT_TRUE(stmt_list2->is_valid);
  auto create_trigger_stmt2 =
      static_cast<parser::CreateStatement *>(stmt_list2->GetStatement(0));
  const planner::CreatePlan plan2(create_trigger_stmt2);
  commands::Trigger trigger2(plan2);
  EXPECT_EQ("check_update_and_delete", trigger2.GetTriggerName());
  int16_t trigger_type2 = trigger2.GetTriggerType();
  EXPECT_TRUE(TRIGGER_FOR_ROW(trigger_type2));
  EXPECT_TRUE(TRIGGER_FOR_BEFORE(trigger_type2));
  EXPECT_TRUE(TRIGGER_FOR_UPDATE(trigger_type2));
  EXPECT_TRUE(TRIGGER_FOR_DELETE(trigger_type2));

  commands::TriggerList trigger_list;
  trigger_list.AddTrigger(trigger1);
  EXPECT_EQ(1, trigger_list.GetTriggerListSize());
  EXPECT_TRUE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_UPDATE_ROW));
  EXPECT_FALSE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_DELETE_ROW));
  EXPECT_FALSE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_INSERT_ROW));

  trigger_list.AddTrigger(trigger2);
  EXPECT_EQ(2, trigger_list.GetTriggerListSize());
  EXPECT_TRUE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_UPDATE_ROW));
  EXPECT_TRUE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_DELETE_ROW));
  EXPECT_FALSE(trigger_list.HasTriggerType(
      commands::EnumTriggerType::BEFORE_INSERT_ROW));
  LOG_INFO("before test ExecBSInsertTriggers");
  trigger_list.ExecBRInsertTriggers();
}
}
}
