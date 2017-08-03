//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// trigger.h
//
// Identification: src/include/commands/trigger.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include "boost/algorithm/string/join.hpp"
#include "expression/abstract_expression.h"
#include "planner/create_plan.h"
#include "common/logger.h"
#include "storage/tuple.h"
#include "expression/tuple_value_expression.h"
#include "expression/comparison_expression.h"
#include "concurrency/transaction_manager.h"
#include "type/types.h"
#include "parser/pg_trigger.h"

namespace peloton {
namespace trigger {

class Trigger;

class TriggerData {
 public:
  // This field is the type of trigger, but it is called `tg_event` in Postgres. Try to
  // keep the variable name same, but not create a new type like TriggerEvent
  int16_t tg_event;
  Trigger *tg_trigger;
  storage::Tuple *tg_trigtuple; // i.e. old tuple
  storage::Tuple *tg_newtuple;

  TriggerData(int16_t tg_event, Trigger *tg_trigger,
              storage::Tuple *tg_trigtuple, storage::Tuple *tg_newtuple) :
    tg_event(tg_event),
    tg_trigger(tg_trigger),
    tg_trigtuple(tg_trigtuple),
    tg_newtuple(tg_newtuple) {}
};

class Trigger {
 public:
  Trigger(const planner::CreatePlan& plan);

  Trigger(std::string name,
          int16_t type,
          std::string function_name,
          std::string arguments,
          const void *fire_condition);

  Trigger(const Trigger& that) {
    trigger_name = that.trigger_name;
    trigger_funcname = that.trigger_funcname;
    trigger_args = that.trigger_args;
    trigger_columns = that.trigger_columns;
    if (that.trigger_when) {
      trigger_when = that.trigger_when->Copy();
    } else {
      trigger_when = nullptr;
    }
    trigger_type = that.trigger_type;
  }

  ~Trigger() {
    if (trigger_when) {
      delete trigger_when;
    }
  }

  int16_t GetTriggerType() { return trigger_type; }

  std::string GetTriggerName() { return trigger_name; }

  storage::Tuple* ExecCallTriggerFunc(TriggerData &trigger_data);

  std::string GetFuncname() { return trigger_funcname; }

  std::string GetArgs() {
    return boost::algorithm::join(trigger_args, ",");
  }

  expression::AbstractExpression* GetTriggerWhen() const {
    return trigger_when;
  }

  //only apply to the simple case: old.balance != new.balance
  void SerializeWhen(SerializeOutput &output, oid_t table_oid,
                     concurrency::Transaction *txn);
  expression::AbstractExpression* DeserializeWhen(SerializeInput &input);

 private:
  std::string trigger_name;
  std::string trigger_funcname;
  std::vector<std::string> trigger_args;
  std::vector<std::string> trigger_columns;
  expression::AbstractExpression* trigger_when = nullptr;
  // information about row, timing, events access by pg_trigger
  int16_t trigger_type;
};


class TriggerList {
 public:
  TriggerList() {/*do nothing*/}

  inline bool HasTriggerType(EnumTriggerType type) const {
    return types_summary[type];
  }

  int GetTriggerListSize() { return static_cast<int>(triggers.size()); }

  void AddTrigger(Trigger trigger);

  void UpdateTypeSummary(int16_t type);

  Trigger* Get(int n) { return &triggers[n]; }  // get trigger by index

  TriggerType ToTriggerType(int16_t trigger_type) {
    int type = 0;
    type |= (trigger_type & TRIGGER_TYPE_ROW) ? TRIGGER_ROW : TRIGGER_STATEMENT;
    type |= (trigger_type & TRIGGER_TYPE_BEFORE) ? TRIGGER_BEFORE : TRIGGER_AFTER;
    type |= (trigger_TYPE & TRIGGER_TYPE_INSERT) ? TRIGGER_INSERT : 0;
    type |= (trigger_TYPE & TRIGGER_TYPE_UPDATE) ? TRIGGER_UPDATE : 0;
    type |= (trigger_TYPE & TRIGGER_TYPE_DELETE) ? TRIGGER_DELETE : 0;
    return static_cast<TriggerType>(type);
  }

  // Execute different types of triggers
  // B/A means before or after
  // R/S means row level or statement level
  // Insert/Update/Delete are events that invoke triggers

  bool ExecTriggers(TriggerType exec_type,
                    storage::Tuple *new_tuple = nullptr,
                    executor::ExecutorContext *executor_context_ = nullptr,
                    storage::Tuple *old_tuple = nullptr,
                    storage::Tuple **resule);

 private:
  // types_summary contains a boolean for each kind of EnumTriggerType, this is
  // used for facilitate checking weather there is a trigger to be invoked
  bool types_summary[TRIGGER_TYPE_MAX] = {false};
  std::vector<Trigger> triggers;
};

}  // namespace trigger
}  // namespace peloton
