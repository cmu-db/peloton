#pragma once

#include "vector"
#include "planner/create_plan.h"

namespace peloton {
namespace commands {

class Trigger {
 public:
  Trigger(const planner::CreatePlan& plan);
  inline int16_t GetTriggerType() { return trigger_type; }
  inline std::string GetTriggerName() { return trigger_name; }
 private:
  std::string trigger_name;
  std::vector<std::string> trigger_funcname;
  std::vector<std::string> trigger_args;
  std::vector<std::string> trigger_columns;
  expression::AbstractExpression* trigger_when;
  int16_t trigger_type; // information about row, timing, events, access by pg_trigger
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
  inline bool HasTriggerType(EnumTriggerType type) const {
    return types_summary[type];
  }
  inline int GetTriggerListSize() { return static_cast<int>(triggers.size()); }
  void AddTrigger(Trigger trigger);
  void UpdateTypeSummary(int16_t type);
 private:
  bool types_summary[TRIGGER_TYPE_MAX] = {false};
  std::vector<Trigger> triggers;

};

}
}