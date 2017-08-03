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
#include "expression/constant_value_expression.h"
#include "catalog/column_catalog.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace commands {
Trigger::Trigger(const peloton::planner::CreatePlan &plan) {
  trigger_name = plan.GetTriggerName();
  trigger_funcname = plan.GetTriggerFuncName();
  trigger_args = plan.GetTriggerArgs();
  trigger_columns = plan.GetTriggerColumns();
  trigger_when = plan.GetTriggerWhen();
  trigger_type = plan.GetTriggerType();
}

//===--------------------------------------------------------------------===//
// DEPRECATED
//===--------------------------------------------------------------------===//
Trigger::Trigger(std::string name,
                 UNUSED_ATTRIBUTE std::string function_name,
                 UNUSED_ATTRIBUTE std::string arguments,
                 UNUSED_ATTRIBUTE std::string fire_condition) {
  trigger_name = name;
  // to be continue...

}
Trigger::Trigger(std::string name,
                 int16_t type,
                 std::string proc_oids,
                 std::string arguments,
                 std::string fire_condition) {
  trigger_name = name;
  std::vector<std::string> strs;
  boost::split(strs, proc_oids, boost::is_any_of(","));
  for (unsigned int i = 0; i < strs.size(); i++) {
    trigger_funcname.push_back(strs[i]);
  }
  strs.clear();
  boost::split(strs, arguments, boost::is_any_of(","));
  for (unsigned int i = 0; i < strs.size(); i++) {
    trigger_args.push_back(strs[i]);
  }
  trigger_type = type;
  trigger_when = DeserializeWhen(fire_condition);
}

std::string Trigger::SerializeWhen(oid_t table_oid, concurrency::Transaction *txn) {
  if (trigger_when == nullptr) {
    return "";
  }
  if (trigger_when->GetChildrenSize() != 2) {
    LOG_ERROR("only simple predicates are supported; \
      for example: NEW.some_columne != some_value or NEW.some_columne = OLD.some_columne");
    return "";
  }
  auto left = trigger_when->GetChild(0);
  auto right = trigger_when->GetChild(1);
  auto compare = trigger_when->GetExpressionType();

  // parse left side of the predicate
  std::string left_side;
  switch (left->GetExpressionType()) {
    case ExpressionType::VALUE_CONSTANT: {
      LOG_DEBUG("left side is value constant");
      auto left_value = static_cast<const expression::ConstantValueExpression *>(left)->GetValue();
      left_side = "left|VALUE_CONSTANT|" + std::to_string(left_value.GetTypeId()) + "|" + left_value.ToString();
      break;
    }
    case ExpressionType::VALUE_TUPLE: {
      // actually, do we need table name?
      std::string left_table = static_cast<const expression::TupleValueExpression *>(left)->GetTableName();
      std::string left_column = static_cast<const expression::TupleValueExpression *>(left)->GetColumnName();
      // get column id
      auto column_id = catalog::ColumnCatalog::GetInstance()->GetColumnId(table_oid, left_column, txn);
      auto column_type = catalog::ColumnCatalog::GetInstance()->GetColumnType(table_oid, left_column, txn);
      left_side =
        "left|VALUE_TUPLE|" + left_table + "|" + std::to_string(column_type) + "|" + std::to_string(column_id);
      break;
    }
    default:break;
  }

  // parse right side of the predicate
  // TODO: should I check whether the value type is the same?? potential bug!!
  std::string right_side;
  switch (right->GetExpressionType()) {
    case ExpressionType::VALUE_CONSTANT: {
      LOG_DEBUG("right side is value constant");
      auto right_value = static_cast<const expression::ConstantValueExpression *>(right)->GetValue();
      right_side = "right|VALUE_CONSTANT|" + std::to_string(right_value.GetTypeId()) + "|" + right_value.ToString();
      break;
    }
    case ExpressionType::VALUE_TUPLE: {
      std::string right_table = static_cast<const expression::TupleValueExpression *>(right)->GetTableName();
      std::string right_column = static_cast<const expression::TupleValueExpression *>(right)->GetColumnName();
      // get column id
      auto column_id = catalog::ColumnCatalog::GetInstance()->GetColumnId(table_oid, right_column, txn);
      auto column_type = catalog::ColumnCatalog::GetInstance()->GetColumnType(table_oid, right_column, txn);
      right_side =
        "right|VALUE_TUPLE|" + right_table + "|" + std::to_string(column_type) + "|" + std::to_string(column_id);
      break;
    }
    default:break;
  }
  LOG_DEBUG("left size of the serialized predicate:%s", left_side.c_str());
  LOG_DEBUG("right size of the serialized predicate:%s", right_side.c_str());
  return ExpressionTypeToString(compare) + "|" + left_side + "|" + right_side;
}

expression::AbstractExpression *Trigger::DeserializeWhen(std::string fire_condition) {
  expression::AbstractExpression *trigger_when = nullptr;
  if (fire_condition == "") {
    return trigger_when;
  }

  //parse expression from string
  std::string s = fire_condition;
  std::string delimiter = "|";
  static std::vector<std::string> v;
  size_t pos = 0;
  std::string token;
  while ((pos = s.find(delimiter)) != std::string::npos) {
    token = s.substr(0, pos);
    v.push_back(token);
    s.erase(0, pos + delimiter.length());
  }
  v.push_back(s);

  if (v.size() <= 0) {
    // TODO: throw exception??
    LOG_ERROR("the format of fire condition is not correct");
    return nullptr;
  }

  std::string left_exp_type, right_exp_type;
  int left_info_begin_index = -1, right_info_begin_index = -1;
  ExpressionType compare = StringToExpressionType(v[0]);
  for (unsigned int i = 0; i < v.size(); i++) {
    if (v[i] == "left" && left_info_begin_index < 0) {
      left_info_begin_index = i + 1;
    }
    // potential bug! what if the table name is "right" ?!
    if (v[i] == "right" && right_info_begin_index < 0) {
      right_info_begin_index = i + 1;
    }
  }

  expression::AbstractExpression *left_exp;
  if (v[left_info_begin_index] == "VALUE_TUPLE") {
    LOG_DEBUG("left side is a tuple value tuple expression");
    // do I need the table nambe?
    auto column_type = atoi(v[left_info_begin_index + 2].c_str());
    auto column_id = atoi(v[left_info_begin_index + 3].c_str());
    // 0 means use the first tuple in the arguments
    left_exp = new expression::TupleValueExpression((type::TypeId) column_type, 0, column_id);

  } else if (v[left_info_begin_index] == "VALUE_CONSTANT") {
    LOG_DEBUG("left side is a value constant expression");
    // potential bug! what if index overflow?
    auto value_type = atoi(v[left_info_begin_index + 1].c_str());
    type::Value left_value;
    switch ((type::TypeId) value_type) {
      case type::TypeId::INTEGER:
        left_value = type::ValueFactory::GetIntegerValue(atoi(v[right_info_begin_index + 2].c_str()));
        break;
      case type::TypeId::TINYINT:
        left_value = type::ValueFactory::GetTinyIntValue(atoi(v[right_info_begin_index + 2].c_str()));
        break;
      case type::TypeId::SMALLINT:
        left_value = type::ValueFactory::GetSmallIntValue(atoi(v[right_info_begin_index + 2].c_str()));
        break;
      case type::TypeId::BIGINT:
        left_value = type::ValueFactory::GetBigIntValue(atoi(v[right_info_begin_index + 2].c_str()));
        break;
      case type::TypeId::DECIMAL:
        left_value = type::ValueFactory::GetDecimalValue(atoi(v[right_info_begin_index + 2].c_str()));
        break;
      default:LOG_ERROR("value type %d is not supported in trigger", (type::TypeId) value_type);
        break;
    }
    left_exp = new expression::ConstantValueExpression(left_value);
  } else {
    // not support
    LOG_ERROR("%s type expression is not supported in trigger", v[left_info_begin_index].c_str());
    return nullptr;
  }

  expression::AbstractExpression *right_exp;
  if (v[right_info_begin_index] == "VALUE_TUPLE") {
    LOG_DEBUG("right side is a value tuple expression");
    auto column_type = atoi(v[right_info_begin_index + 2].c_str());
    auto column_id = atoi(v[right_info_begin_index + 3].c_str());
    // 1 means use the second tuple in the arguments
    right_exp = new expression::TupleValueExpression((type::TypeId) column_type, 1, column_id);

  } else if (v[right_info_begin_index] == "VALUE_CONSTANT") {
    LOG_DEBUG("right side is a value constant expression");
    // potential bug! what if index overflow?
    auto value_type = atoi(v[right_info_begin_index + 1].c_str());
    type::Value right_value;
    switch ((type::TypeId) value_type) {
      case type::TypeId::INTEGER:
        right_value = type::ValueFactory::GetIntegerValue(atoi(v[right_info_begin_index + 2].c_str()));
        break;
      case type::TypeId::TINYINT:
        right_value = type::ValueFactory::GetTinyIntValue(atoi(v[right_info_begin_index + 2].c_str()));
        break;
      case type::TypeId::SMALLINT:
        right_value = type::ValueFactory::GetSmallIntValue(atoi(v[right_info_begin_index + 2].c_str()));
        break;
      case type::TypeId::BIGINT:
        right_value = type::ValueFactory::GetBigIntValue(atoi(v[right_info_begin_index + 2].c_str()));
        break;
      case type::TypeId::DECIMAL:
        right_value = type::ValueFactory::GetDecimalValue(atoi(v[right_info_begin_index + 2].c_str()));
        break;
      default:LOG_ERROR("value type %d is not supported in trigger", (type::TypeId) value_type);
        break;
    }
    right_exp = new expression::ConstantValueExpression(right_value);
  } else {
    // not support
    LOG_ERROR("%s type expression is not supported in trigger", v[left_info_begin_index].c_str());
    return nullptr;
  }

  //construct expression
  auto final_exp = new expression::ComparisonExpression(compare, left_exp, right_exp);
  LOG_DEBUG("successfully construct the final predicate!");
  return final_exp;
}

/*
 * Add a trigger to the trigger list and update the summary
 */
void TriggerList::AddTrigger(Trigger trigger) {
  LOG_DEBUG("enter AddTrigger()");
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
storage::Tuple *TriggerList::ExecBRInsertTriggers(storage::Tuple *tuple, executor::ExecutorContext *executor_context_) {
  unsigned i;
  LOG_DEBUG("enter into ExecBRInsertTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::BEFORE_INSERT_ROW]) {
    return nullptr;
  }

  storage::Tuple *new_tuple = tuple;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT)) {
      continue;
    }

    //TODO: check if trigger is enabled

    expression::AbstractExpression *predicate_ = obj.GetTriggerWhen();
    if (predicate_ != nullptr) {
      LOG_DEBUG("predicate_ is not nullptr");
      LOG_DEBUG("predicate_ type = %s", ExpressionTypeToString(predicate_->GetExpressionType()).c_str());
      LOG_DEBUG("predicate_ size = %lu", predicate_->GetChildrenSize());
      if (executor_context_ != nullptr) {
        LOG_DEBUG("before evalulate");
        auto tuple_new = (const AbstractTuple *) tuple;
        auto eval = predicate_->Evaluate(tuple_new, nullptr, executor_context_);
        LOG_DEBUG("Evaluation result: %s", eval.GetInfo().c_str());
        if (eval.IsTrue()) {
          LOG_DEBUG("pass one trigger fire condition!!!");
          LOG_DEBUG("trigger %s is fired!", triggers[i].GetTriggerName().c_str());
        } else {
          LOG_DEBUG("fail one trigger fire condition!!!");
          continue;
        }
      }
    } else {
      LOG_DEBUG("predicate_ is nullptr");
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, tuple);

    // apply all per-row-before-insert triggers on the tuple
    new_tuple = obj.ExecCallTriggerFunc(trigger_data);
  }
  return new_tuple;
}

storage::Tuple *TriggerList::ExecARInsertTriggers(storage::Tuple *tuple, executor::ExecutorContext *executor_context_) {
  unsigned i;
  LOG_DEBUG("enter into ExecARInsertTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::AFTER_INSERT_ROW]) {
    return nullptr;
  }

  storage::Tuple *new_tuple = tuple;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT)) {
      continue;
    }

    expression::AbstractExpression *predicate_ = obj.GetTriggerWhen();
    if (predicate_ != nullptr) {
      LOG_DEBUG("predicate_ is not nullptr");
      LOG_DEBUG("predicate_ type = %s", ExpressionTypeToString(predicate_->GetExpressionType()).c_str());
      LOG_DEBUG("predicate_ size = %lu", predicate_->GetChildrenSize());
      if (executor_context_ != nullptr) {
        LOG_DEBUG("before evalulate");
        auto tuple_new = (const AbstractTuple *) tuple;
        auto eval = predicate_->Evaluate(tuple_new, nullptr, executor_context_);
        LOG_DEBUG("Evaluation result: %s", eval.GetInfo().c_str());
        if (eval.IsTrue()) {
          LOG_DEBUG("pass one trigger fire condition!!!");
          LOG_DEBUG("trigger %s is fired!", triggers[i].GetTriggerName().c_str());
        } else {
          LOG_DEBUG("fail one trigger fire condition!!!");
          continue;
        }
      }
    } else {
      LOG_DEBUG("predicate_ is nullptr");
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, tuple);

    // apply all per-row-after-insert triggers on the tuple
    new_tuple = obj.ExecCallTriggerFunc(trigger_data);
  }
  return new_tuple;
}

bool TriggerList::ExecBRUpdateTriggers() {
  LOG_DEBUG("enter into ExecBRUpdateTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::BEFORE_UPDATE_ROW]) {
    return false;
  }
  unsigned i;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE)) {
      continue;
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, nullptr);

    // apply all per-row-before-update triggers on the table
    obj.ExecCallTriggerFunc(trigger_data);
  }
  return true;
}

bool TriggerList::ExecARUpdateTriggers(storage::Tuple *new_tuple,
                                       storage::Tuple *old_tuple,
                                       executor::ExecutorContext *executor_context_) {
  LOG_DEBUG("enter into ExecARUpdateTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::AFTER_UPDATE_ROW]) {
    return false;
  }
  unsigned i;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE)) {
      continue;
    }

    expression::AbstractExpression *predicate_ = obj.GetTriggerWhen();
    if (predicate_ != nullptr) {
      LOG_DEBUG("predicate_ is not nullptr");
      LOG_DEBUG("predicate_ type = %s", ExpressionTypeToString(predicate_->GetExpressionType()).c_str());
      LOG_DEBUG("predicate_ size = %lu", predicate_->GetChildrenSize());
      if (executor_context_ != nullptr) {
        LOG_DEBUG("before evalulate");
        auto eval = predicate_->Evaluate(new_tuple, old_tuple, executor_context_);
        LOG_DEBUG("Evaluation result: %s", eval.GetInfo().c_str());
        if (eval.IsTrue()) {
          LOG_DEBUG("pass one trigger fire condition!!!");
          LOG_DEBUG("trigger %s is fired!", triggers[i].GetTriggerName().c_str());
        } else {
          LOG_DEBUG("fail one trigger fire condition!!!");
          continue;
        }
      }
    } else {
      LOG_DEBUG("predicate_ is nullptr");
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, nullptr);

    // apply all per-row-after-update triggers on the table
    obj.ExecCallTriggerFunc(trigger_data);
  }
  return true;
}

bool TriggerList::ExecBRDeleteTriggers(storage::Tuple *tuple, executor::ExecutorContext *executor_context_) {
  LOG_DEBUG("enter into ExecBRDeleteTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::BEFORE_DELETE_ROW]) {
    return false;
  }
  unsigned i;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE)) {
      continue;
    }

    expression::AbstractExpression *predicate_ = obj.GetTriggerWhen();
    if (predicate_ != nullptr) {
      LOG_DEBUG("predicate_ is not nullptr");
      LOG_DEBUG("predicate_ type = %s", ExpressionTypeToString(predicate_->GetExpressionType()).c_str());
      LOG_DEBUG("predicate_ size = %lu", predicate_->GetChildrenSize());
      if (executor_context_ != nullptr) {
        LOG_DEBUG("before evalulate");
        auto tuple_new = (const AbstractTuple *) tuple;
        auto eval = predicate_->Evaluate(tuple_new, nullptr, executor_context_);
        LOG_DEBUG("Evaluation result: %s", eval.GetInfo().c_str());
        if (eval.IsTrue()) {
          LOG_DEBUG("pass one trigger fire condition!!!");
          LOG_DEBUG("trigger %s is fired!", triggers[i].GetTriggerName().c_str());
        } else {
          LOG_DEBUG("fail one trigger fire condition!!!");
          continue;
        }
      }
    } else {
      LOG_DEBUG("predicate_ is nullptr");
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, nullptr);

    // apply all per-row-before-delete triggers on the table
    obj.ExecCallTriggerFunc(trigger_data);
  }
  return true;
}

bool TriggerList::ExecARDeleteTriggers(storage::Tuple *tuple, executor::ExecutorContext *executor_context_) {
  LOG_DEBUG("enter into ExecARDeleteTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::AFTER_DELETE_ROW]) {
    return false;
  }
  unsigned i;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE)) {
      continue;
    }

    expression::AbstractExpression *predicate_ = obj.GetTriggerWhen();
    if (predicate_ != nullptr) {
      LOG_DEBUG("predicate_ is not nullptr");
      LOG_DEBUG("predicate_ type = %s", ExpressionTypeToString(predicate_->GetExpressionType()).c_str());
      LOG_DEBUG("predicate_ size = %lu", predicate_->GetChildrenSize());
      if (executor_context_ != nullptr) {
        LOG_DEBUG("before evalulate");
        auto tuple_new = (const AbstractTuple *) tuple;
        auto eval = predicate_->Evaluate(tuple_new, nullptr, executor_context_);
        LOG_DEBUG("Evaluation result: %s", eval.GetInfo().c_str());
        if (eval.IsTrue()) {
          LOG_DEBUG("pass one trigger fire condition!!!");
          LOG_DEBUG("trigger %s is fired!", triggers[i].GetTriggerName().c_str());
        } else {
          LOG_DEBUG("fail one trigger fire condition!!!");
          continue;
        }
      }
    } else {
      LOG_DEBUG("predicate_ is nullptr");
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, nullptr);

    // apply all per-row-after-delete triggers on the table
    obj.ExecCallTriggerFunc(trigger_data);
  }
  return true;
}

void TriggerList::ExecBSInsertTriggers() {
  LOG_DEBUG("enter into ExecBSInsertTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::BEFORE_INSERT_STATEMENT]) {
    return;
  }
  unsigned i;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT)) {
      continue;
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, nullptr);

    // apply all per-statement-before-insert triggers on the table
    obj.ExecCallTriggerFunc(trigger_data);
  }
  return;
}

void TriggerList::ExecASInsertTriggers() {
  LOG_DEBUG("enter into ExecASInsertTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::AFTER_INSERT_STATEMENT]) {
    return;
  }
  unsigned i;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT)) {
      continue;
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, nullptr);

    // apply all per-statement-after-insert triggers on the table
    obj.ExecCallTriggerFunc(trigger_data);
  }
  return;
}

bool TriggerList::ExecBSUpdateTriggers() {
  LOG_DEBUG("enter into ExecBSUpdateTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::BEFORE_UPDATE_STATEMENT]) {
    return false;
  }
  unsigned i;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE)) {
      continue;
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, nullptr);

    // apply all per-statement-before-update triggers on the table
    obj.ExecCallTriggerFunc(trigger_data);
  }
  return true;
}

bool TriggerList::ExecASUpdateTriggers() {
  LOG_DEBUG("enter into ExecASUpdateTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::AFTER_UPDATE_STATEMENT]) {
    return false;
  }
  unsigned i;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE)) {
      continue;
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, nullptr);

    // apply all per-statement-after-update triggers on the table
    obj.ExecCallTriggerFunc(trigger_data);
  }
  return true;
}

bool TriggerList::ExecBSDeleteTriggers() {
  LOG_DEBUG("enter into ExecBSDeleteTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::BEFORE_DELETE_STATEMENT]) {
    return false;
  }
  unsigned i;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE)) {
      continue;
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, nullptr);

    // apply all per-statement-before-delete triggers on the table
    obj.ExecCallTriggerFunc(trigger_data);
  }
  return true;
}

bool TriggerList::ExecASDeleteTriggers() {
  LOG_DEBUG("enter into ExecASDeleteTriggers");

  //check valid type
  if (!types_summary[EnumTriggerType::AFTER_DELETE_STATEMENT]) {
    return false;
  }
  unsigned i;
  for (i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    //check valid type
    if (!TRIGGER_TYPE_MATCHES(trigger_type, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE)) {
      continue;
    }

    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, nullptr, nullptr);

    // apply all per-statement-after-delete triggers on the table
    obj.ExecCallTriggerFunc(trigger_data);
  }
  return true;
}

/**
 * Call a trigger function.
 */
storage::Tuple *Trigger::ExecCallTriggerFunc(TriggerData &trigger_data) {
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
  return trigger_data.tg_newtuple;
}

}
}
