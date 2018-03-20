//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// trigger.cpp
//
// Identification: src/trigger/trigger.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "trigger/trigger.h"

#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/table_catalog.h"
#include "concurrency/transaction_context.h"
#include "expression/comparison_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"

namespace peloton {
namespace trigger {

Trigger::Trigger(const peloton::planner::CreatePlan &plan) {
  trigger_name = plan.GetTriggerName();
  trigger_funcname = plan.GetTriggerFuncName()[0];
  trigger_args = plan.GetTriggerArgs();
  trigger_columns = plan.GetTriggerColumns();
  trigger_when = plan.GetTriggerWhen();
  trigger_type = plan.GetTriggerType();
}

Trigger::Trigger(const std::string &name, int16_t type,
                 const std::string &function_name, const std::string &arguments,
                 const void *fire_condition) {
  trigger_name = name;
  trigger_type = type;
  trigger_funcname = function_name;
  std::vector<std::string> strs;
  boost::split(strs, arguments, boost::is_any_of(","));
  for (unsigned int i = 0; i < strs.size(); i++) {
    trigger_args.push_back(strs[i]);
  }
  int size = *(int *)fire_condition;
  CopySerializeInput input(fire_condition, size);
  trigger_when = DeserializeWhen(input);
}

/**
 * The expression is serialized as:
 *
 * [(int) total size]
 * [compare type]
 * [left expr type] ( [type] [value] | [column type] [column id] )
 * [right expr type] ( [type] [value] | [column type] [column id] )
 *
 */
void Trigger::SerializeWhen(SerializeOutput &output, oid_t database_oid,
                            oid_t table_oid, concurrency::TransactionContext *txn) {
  size_t start = output.Position();
  output.WriteInt(0);  // reserve first 4 bytes for the total tuple size
  if (trigger_when != nullptr) {
    if (trigger_when->GetChildrenSize() != 2) {
      LOG_ERROR(
          "only simple predicates are supported; "
          "for example: NEW.some_columne != some_value or "
          "NEW.some_columne = OLD.some_columne");
    } else {
      auto left = trigger_when->GetChild(0);
      auto right = trigger_when->GetChild(1);
      auto compare = trigger_when->GetExpressionType();

      output.WriteInt(static_cast<int>(compare));

      std::vector<const expression::AbstractExpression *> exprs = {left, right};

      for (auto expr : exprs) {
        output.WriteInt(static_cast<int>(expr->GetExpressionType()));
        switch (expr->GetExpressionType()) {
          case ExpressionType::VALUE_CONSTANT: {
            auto value =
                static_cast<const expression::ConstantValueExpression *>(expr)
                    ->GetValue();
            output.WriteInt(static_cast<int>(value.GetTypeId()));
            value.SerializeTo(output);
            break;
          }
          case ExpressionType::VALUE_TUPLE: {
            auto e =
                static_cast<const expression::TupleValueExpression *>(expr);
            auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
                database_oid, table_oid, txn);
            auto column_object =
                table_object->GetColumnObject(e->GetColumnName());
            output.WriteInt(static_cast<int>(column_object->GetColumnType()));
            output.WriteInt(static_cast<int>(column_object->GetColumnId()));
            break;
          }
          default:
            break;
        }
      }
    }
  }
  int32_t total_size = static_cast<int32_t>(output.Position() - start);
  output.WriteIntAt(start, total_size);
}

expression::AbstractExpression *Trigger::DeserializeWhen(
    SerializeInput &input) {
  expression::AbstractExpression *trigger_when = nullptr;
  int total_size = input.ReadInt();
  if ((unsigned)total_size > sizeof(int)) {
    ExpressionType compare = ExpressionType(input.ReadInt());
    std::vector<expression::AbstractExpression *> exprs;
    for (int i = 0; i < 2; ++i) {
      expression::AbstractExpression *expr = nullptr;
      ExpressionType expr_type = ExpressionType(input.ReadInt());
      switch (expr_type) {
        case ExpressionType::VALUE_CONSTANT: {
          type::TypeId value_type = type::TypeId(input.ReadInt());
          auto value = type::Value::DeserializeFrom(input, value_type);
          expr = new expression::ConstantValueExpression(value);
          break;
        }
        case ExpressionType::VALUE_TUPLE: {
          type::TypeId column_type = type::TypeId(input.ReadInt());
          int column_id = input.ReadInt();
          expr =
              new expression::TupleValueExpression(column_type, 0, column_id);
          break;
        }
        default:
          LOG_ERROR("%s type expression is not supported in trigger",
                    ExpressionTypeToString(expr_type).c_str());
          return nullptr;
      }
      exprs.push_back(expr);
    }
    trigger_when =
        new expression::ComparisonExpression(compare, exprs[0], exprs[1]);
  }
  return trigger_when;
}

/*
 * Add a trigger to the trigger list and update the summary
 */
void TriggerList::AddTrigger(Trigger trigger) {
  triggers.push_back(trigger);
  UpdateTypeSummary(trigger.GetTriggerType());
}

/*
 * Update a type summary when a new type of trigger is added
 */
void TriggerList::UpdateTypeSummary(int16_t type) {
  for (int t = 0; t <= TRIGGER_TYPE_MAX; ++t) {
    if (CheckTriggerType(type, TriggerType(t))) {
      types_summary[t] = true;
    }
  }
}

bool TriggerList::ExecTriggers(TriggerType exec_type,
                               concurrency::TransactionContext *txn,
                               storage::Tuple *new_tuple,
                               executor::ExecutorContext *executor_context_,
                               storage::Tuple *old_tuple,
                               const storage::Tuple **result) {
  if (!types_summary[static_cast<int>(exec_type)]) {
    if (result != nullptr) *result = nullptr;
    return false;
  }

  if (result != nullptr) *result = new_tuple;

  for (unsigned i = 0; i < triggers.size(); i++) {
    Trigger &obj = triggers[i];
    int16_t trigger_type = obj.GetTriggerType();
    // check valid type
    if (!CheckTriggerType(trigger_type, exec_type)) continue;

    // TODO: check if trigger is enabled

    expression::AbstractExpression *predicate_ = obj.GetTriggerWhen();
    if (predicate_ != nullptr) {
      if (executor_context_ != nullptr) {
        auto tuple_new = (const AbstractTuple *)new_tuple;
        auto tuple_old = (const AbstractTuple *)old_tuple;
        auto eval =
            predicate_->Evaluate(tuple_new, tuple_old, executor_context_);
        if (!eval.IsTrue()) continue;
      }
    }
    // Construct trigger data
    TriggerData trigger_data(trigger_type, &obj, old_tuple, new_tuple);

    if (IsOnCommit(exec_type)) {
      PL_ASSERT(txn != nullptr);
      txn->AddOnCommitTrigger(trigger_data);
    } else {
      // apply all triggers on the tuple
      storage::Tuple *ret = obj.ExecCallTriggerFunc(trigger_data);
      if (result) *result = ret;
    }
  }
  return true;
}

/**
 * Call a trigger function.
 */
storage::Tuple *Trigger::ExecCallTriggerFunc(TriggerData &trigger_data) {
  std::string &trigger_name = trigger_data.tg_trigger->trigger_name;
  std::string &trigger_funcname = trigger_data.tg_trigger->trigger_funcname;
  LOG_INFO("Trigger %s is invoked", trigger_name.c_str());
  LOG_INFO("Function %s should be called", trigger_funcname.c_str());
  // TODO: It should call UDF function here.
  // One concern is that UDF is not supported in the master branch currently.
  // Another concern is that currently UDF is mainly designed for read-only
  // operations without SQL statements, but mostly, functions invoked by a
  // trigger need to apply SQL statements on databases. Hope
  // `ExecCallTriggerFunc`
  // could be truly implemented after these problems are resolved.
  return trigger_data.tg_newtuple;
}

}  // namespace trigger
}  // namespace peloton
