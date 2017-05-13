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
  // TODO: It should call UDF function. This could be implemented after UDF is supported in
  // the master branch. Another concern is that currently UDF looks like only support read only
  // operations, but usually functions invoked by a trigger need to apply SQL statements on
  // databases.
  return nullptr;
}

}
}
