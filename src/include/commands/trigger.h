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

#include "vector"
#include "boost/algorithm/string/join.hpp"
#include "expression/abstract_expression.h"
#include "planner/create_plan.h"
#include "common/logger.h"
#include "storage/tuple.h"
#include "expression/tuple_value_expression.h"
#include "expression/comparison_expression.h"
#include "concurrency/transaction_manager.h"

namespace peloton {
namespace commands {

class Trigger;

class TriggerData {
 public:
  // This field is the type of trigger, but it is called `tg_event` in Postgres. Try to
  // keep the variable name same, but not create a new type like TriggerEvent
  int16_t tg_event;
  Trigger *tg_trigger;
  storage::Tuple *tg_trigtuple; // i.e. old tuple
  storage::Tuple * tg_newtuple;

  TriggerData(int16_t tg_event, Trigger *tg_trigger, storage::Tuple *tg_trigtuple, storage::Tuple *tg_newtuple) :
    tg_event(tg_event), tg_trigger(tg_trigger), tg_trigtuple(tg_trigtuple), tg_newtuple(tg_newtuple) {}
};

class Trigger {
 public:
  Trigger(const planner::CreatePlan& plan);
  Trigger(const Trigger& that) {
    trigger_name = that.trigger_name;
    trigger_funcname = that.trigger_funcname;
    trigger_args = that.trigger_args;
    trigger_columns = that.trigger_columns;
    // trigger_when = that.trigger_when->Copy();
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
  Trigger(std::string name,
          UNUSED_ATTRIBUTE std::string function_name,
          UNUSED_ATTRIBUTE std::string arguments,
          UNUSED_ATTRIBUTE std::string fire_condition);
  Trigger(std::string name,
          int16_t type,
          UNUSED_ATTRIBUTE std::string function_name,
          UNUSED_ATTRIBUTE std::string arguments,
          UNUSED_ATTRIBUTE std::string fire_condition);
  inline int16_t GetTriggerType() { return trigger_type; }
  inline std::string GetTriggerName() { return trigger_name; }
  storage::Tuple* ExecCallTriggerFunc(TriggerData &trigger_data);

  inline std::string GetFuncname() {
    return boost::algorithm::join(trigger_funcname, ",");
  }

  inline std::string GetArgs() {
    return boost::algorithm::join(trigger_args, ",");
  }

  //TODO
  inline std::string GetWhen() {return "function_arguments";}

  //only apply to the simple case: old.balance != new.balance
  std::string SerializeWhen(oid_t table_oid, concurrency::Transaction *txn);
  expression::AbstractExpression* DeserializeWhen(std::string fire_condition);

  inline expression::AbstractExpression* GetTriggerWhen() const {return trigger_when;}

 private:
  std::string trigger_name;
  std::vector<std::string> trigger_funcname;
  std::vector<std::string> trigger_args;
  std::vector<std::string> trigger_columns;
  expression::AbstractExpression* trigger_when = nullptr;
  int16_t trigger_type;  // information about row, timing, events, access by
                         // pg_trigger
};

typedef enum TriggerType {
  BEFORE_INSERT_ROW = 0,
  BEFORE_INSERT_STATEMENT,
  BEFORE_UPDATE_ROW,
  BEFORE_UPDATE_STATEMENT,
  BEFORE_DELETE_ROW,
  BEFORE_DELETE_STATEMENT,
  AFTER_INSERT_ROW,
  AFTER_INSERT_STATEMENT,
  AFTER_UPDATE_ROW,
  AFTER_UPDATE_STATEMENT,
  AFTER_DELETE_ROW,
  AFTER_DELETE_STATEMENT,
  TRIGGER_TYPE_MAX  // for counting the number of trigger types
} EnumTriggerType;

class TriggerList {
 public:
  TriggerList() {/*do nothing*/}
  inline bool HasTriggerType(EnumTriggerType type) const {
    return types_summary[type];
  }
  inline int GetTriggerListSize() { return static_cast<int>(triggers.size()); }
  void AddTrigger(Trigger trigger);
  void UpdateTypeSummary(int16_t type);
  Trigger* Get(int n) { return &triggers[n]; }  // get trigger by index
  storage::Tuple* ExecBRInsertTriggers(storage::Tuple *new_tuple, executor::ExecutorContext *executor_context_);
 private:
  // types_summary contains a boolean for each kind of EnumTriggerType, this is
  // used for facilitate checking weather there is a trigger to be invoked
  bool types_summary[TRIGGER_TYPE_MAX] = {false};
  std::vector<Trigger> triggers;
};
}
}
