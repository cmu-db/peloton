//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// trigger.cpp
//
// Identification: src/commands/trigger.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "commands/trigger.h"
#include "parser/pg_trigger.h"
#include "common/logger.h"

namespace peloton {
namespace commands {
Trigger::Trigger(const peloton::planner::CreatePlan& plan) {
  trigger_name = plan.GetTriggerName();
  trigger_funcname = plan.GetTriggerFuncName();
  trigger_args = plan.GetTriggerArgs();
  trigger_columns = plan.GetTriggerColumns();
  trigger_when = plan.GetTriggerWhen();
  trigger_type = plan.GetTriggerType();
}

// TODO:
Trigger::Trigger(std::string name, UNUSED_ATTRIBUTE std::string function_name, UNUSED_ATTRIBUTE std::string arguments, UNUSED_ATTRIBUTE std::string fire_condition) {
  trigger_name = name;
  // to be continue...

}
Trigger::Trigger(std::string name, int16_t type, UNUSED_ATTRIBUTE std::string function_name, UNUSED_ATTRIBUTE std::string arguments, UNUSED_ATTRIBUTE std::string fire_condition) {
  trigger_name = name;
  // to be continue...
  // if (type == (TRIGGER_TYPE_ROW|TRIGGER_TYPE_BEFORE|TRIGGER_TYPE_DELETE)) {
  //   trigger_type = BEFORE_DELETE_ROW;
  // } else {
  //   // probabily not a good way to construct trigger_type
  //   // TODO:
  // }
  trigger_type = type;
  trigger_when = DeserializeWhen(fire_condition);
}

std::string Trigger::SerializeWhen()
{
  if (trigger_when == nullptr) {
    return "";
  }
  if (trigger_when->GetExpressionType() != ExpressionType::COMPARE_NOTEQUAL) {
    return "";
  }
  if (trigger_when->GetChildrenSize() != 2) {
    return "";
  }
  auto left = trigger_when->GetChild(0);
  auto right = trigger_when->GetChild(1);
  auto compare = trigger_when->GetExpressionType();
  if (left->GetExpressionType() != ExpressionType::VALUE_TUPLE || right->GetExpressionType() != ExpressionType::VALUE_TUPLE) {
    return "";
  }
  std::string left_table = static_cast<const expression::TupleValueExpression *>(left)->GetTableName();
  std::string left_column = static_cast<const expression::TupleValueExpression *>(left)->GetColumnName();
  std::string right_table = static_cast<const expression::TupleValueExpression *>(right)->GetTableName();
  std::string right_column = static_cast<const expression::TupleValueExpression *>(right)->GetColumnName();
  return ExpressionTypeToString(compare) + "-" + left_table + "-" + left_column + "-" + right_table + "-" + right_column;
}

expression::AbstractExpression* Trigger::DeserializeWhen(std::string fire_condition) {
  expression::AbstractExpression* trigger_when = nullptr;
  if (fire_condition == "") {
    return trigger_when;
  }

  //parse expression from string
  std::string s = fire_condition;
  std::string delimiter = "-";
  static std::vector<std::string> v;
  size_t pos = 0;
  std::string token;
  while ((pos = s.find(delimiter)) != std::string::npos) {
    token = s.substr(0, pos);
    v.push_back(token);
    s.erase(0, pos + delimiter.length());
  }
  v.push_back(s);

  // ExpressionType compare = StringToExpressionType(v[0]);
  std::string left_table = v[1];
  std::string left_column = v[2];
  std::string right_table = v[3];
  std::string right_column = v[4];

  //construct expression
  auto left_exp = new expression::TupleValueExpression(std::move(left_column), std::move(left_table));
  auto right_exp = new expression::TupleValueExpression(std::move(right_column), std::move(right_table));
  // expression::ComparisonExpression compare_exp(ExpressionType::COMPARE_NOTEQUAL, left_exp, right_exp);
  auto compare_exp = new expression::ComparisonExpression(ExpressionType::COMPARE_NOTEQUAL, left_exp, right_exp);
  return compare_exp;
}

/*
 * Add a trigger to the trigger list and update the summary
 */
void TriggerList::AddTrigger(Trigger trigger) {
  LOG_INFO("enter AddTrigger()");
  triggers.push_back(trigger);
  UpdateTypeSummary(trigger.GetTriggerType());
}

/*
 * Update a type summary when a new type of trigger is added
 */
void TriggerList::UpdateTypeSummary(int16_t type) {
  types_summary[BEFORE_INSERT_ROW] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT);
  types_summary[BEFORE_INSERT_STATEMENT] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT);
  types_summary[BEFORE_UPDATE_ROW] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE);
  types_summary[BEFORE_UPDATE_STATEMENT] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE);
  types_summary[BEFORE_DELETE_ROW] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE);
  types_summary[BEFORE_DELETE_STATEMENT] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE);
  types_summary[AFTER_INSERT_ROW] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT);
  types_summary[AFTER_INSERT_STATEMENT] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT);
  types_summary[AFTER_UPDATE_ROW] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE);
  types_summary[AFTER_UPDATE_STATEMENT] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE);
  types_summary[AFTER_DELETE_ROW] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE);
  types_summary[AFTER_DELETE_STATEMENT] |= TRIGGER_TYPE_MATCHES(
      type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE);
}

/**
 * execute trigger on each row before inserting tuple.
 */
storage::Tuple* TriggerList::ExecBRInsertTriggers(storage::Tuple *tuple) {
  unsigned i;
  LOG_INFO("enter into ExecBRInsertTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::BEFORE_INSERT_ROW]) {
    return nullptr;
  }

  storage::Tuple* new_tuple = tuple;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT)) {
      continue;
    }

    //TODO: check if trigger is enabled

    //TODO: check trigger fire condition
    expression::AbstractExpression* predicate_ = obj.GetTriggerWhen();
    if (predicate_ != nullptr) {
      LOG_INFO("predicate_ is not nullptr");
      LOG_INFO("predicate_ type = %s", ExpressionTypeToString(predicate_->GetExpressionType()).c_str());
      LOG_INFO("predicate_ size = %lu", predicate_->GetChildrenSize());
      if (executor_context_ != nullptr) {
        LOG_INFO("before evalulate");
        auto tuple_new = (const AbstractTuple *) tuple;
        LOG_INFO("step1");
        auto eval = predicate_->Evaluate(tuple_new, nullptr, executor_context_);
        LOG_INFO("Evaluation result: %s", eval.GetInfo().c_str());
        if (eval.IsTrue()) {
          continue;
        }
      }
    } else {
      LOG_INFO("predicate_ is nullptr");
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, tuple);

    // apply all per-row-before-insert triggers on the tuple
    new_tuple = obj.ExecCallTriggerFunc(trigger_data);
  }
  return new_tuple;
}

/**
 * Call a trigger function.
 */
storage::Tuple* Trigger::ExecCallTriggerFunc(TriggerData &trigger_data) {
  std::string &trigger_name = trigger_data.tg_trigger->trigger_name;
  std::string &trigger_funcname = trigger_data.tg_trigger->trigger_funcname[0];
  LOG_INFO("Trigger %s is invoked", trigger_name.c_str());
  LOG_INFO("Function %s should be called", trigger_funcname.c_str());
  // TODO: It should call UDF function here.
  // One concern is that UDF is not supported in the master branch currently.
  // Another concern is that currently UDF is mainly designed for read-only
  // operations without SQL statements, but mostly, functions invoked by a
  // trigger need to apply SQL statements on databases. Hope `ExecCallTriggerFunc`
  // could be truly implemented after these problems are resolved.
  return nullptr;
}

}
}
