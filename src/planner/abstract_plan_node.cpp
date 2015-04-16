/**
 * @brief Base class for all plan nodes.
 *
 * Copyright(c) 2015, CMU
 */

#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/types.h"
#include "planner/abstract_plan_node.h"
#include "planner/plan_column.h"
#include "planner/plan_node_util.h"

namespace nstore {
namespace planner {

AbstractPlanNode::AbstractPlanNode(oid_t plan_node_id)
: plan_node_id(plan_node_id),
  executor(nullptr),
  output(nullptr),
  is_inlined(false) {
}

AbstractPlanNode::AbstractPlanNode()
: plan_node_id(-1),
  executor(nullptr),
  output(nullptr),
  is_inlined(false) {
}

AbstractPlanNode::~AbstractPlanNode() {
  // clean up executor
  delete executor;

  // clean up inlined nodes
  for (auto node : inlined_nodes){
    delete node.second;
  }
}

//===--------------------------------------------------------------------===//
// Children + Parent Helpers
//===--------------------------------------------------------------------===//

void AbstractPlanNode::AddChild(AbstractPlanNode* child) {
  children.push_back(child);
}

std::vector<AbstractPlanNode*>& AbstractPlanNode::GetChildren() {
  return children;
}

std::vector<oid_t>& AbstractPlanNode::GetChildrenIds(){
  return children_ids;
}

const std::vector<AbstractPlanNode*>& AbstractPlanNode::GetChildren() const {
  return children;
}

void AbstractPlanNode::AddParent(AbstractPlanNode* parent){
  parents.push_back(parent);
}

std::vector<AbstractPlanNode*>& AbstractPlanNode::GetParents(){
  return parents;
}

std::vector<oid_t>& AbstractPlanNode::GetParentIds() {
  return parent_ids;
}

const std::vector<AbstractPlanNode*>& AbstractPlanNode::GetParents() const {
  return parents;
}

//===--------------------------------------------------------------------===//
// Inlined plannodes
//===--------------------------------------------------------------------===//

void AbstractPlanNode::AddInlinePlanNode(AbstractPlanNode* inline_node) {
  inlined_nodes[inline_node->GetPlanNodeType()] = inline_node;
  inline_node->is_inlined = true;
}

AbstractPlanNode* AbstractPlanNode::GetInlinePlanNodes(PlanNodeType type) const {

  auto lookup = inlined_nodes.find(type);
  AbstractPlanNode* ret = nullptr;

  if (lookup != inlined_nodes.end()) {
    ret = lookup->second;
  }
  else {
    //VOLT_TRACE("No internal PlanNode with type '%s' is available for '%s'",
    //           plannodeutil::getTypeName(type).c_str(),
    //           this->debug().c_str());
  }

  return ret;
}

std::map<PlanNodeType, AbstractPlanNode*>& AbstractPlanNode::GetInlinePlanNodes() {
  return inlined_nodes;
}

const std::map<PlanNodeType, AbstractPlanNode*>& AbstractPlanNode::GetInlinePlanNodes() const{
  return inlined_nodes;
}

bool AbstractPlanNode::IsInlined() const {
  return is_inlined;
}

//===--------------------------------------------------------------------===//
// Accessors
//===--------------------------------------------------------------------===//

void AbstractPlanNode::SetPlanNodeId(oid_t plan_node_id_) {
  plan_node_id = plan_node_id_;
}

oid_t AbstractPlanNode::GetPlanNodeId() const {
  return plan_node_id;
}

void AbstractPlanNode::SetExecutor(executor::AbstractExecutor* executor_) {
  executor = executor_;
}

void AbstractPlanNode::SetInputs(const std::vector<LogicalTile*>& val){
  inputs = val;
}

std::vector<LogicalTile*>& AbstractPlanNode::GetInputs() {
  return inputs;
}

void AbstractPlanNode::SetOutput(LogicalTile* table) {
  output = table;
}

LogicalTile* AbstractPlanNode::GetOutput() const {
  return output;
}

std::vector<oid_t> AbstractPlanNode::GetOutputColumnGuids() const {
  return output_column_guids;
}

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

AbstractPlanNode* AbstractPlanNode::LoadFromJSONObject(json_spirit::Object &obj,
                                                       const catalog::Database *catalog_db) {

  json_spirit::Value value_type = find_value(obj, "PLAN_NODE_TYPE");

  if (value_type == json_spirit::Value::null){
    throw PlannerException("AbstractPlanNode::fromJSONObject: PLAN_NODE_TYPE value is null");
  }

  std::string type_string = value_type.get_str();
  AbstractPlanNode* plan_node = GetEmptyPlanNode(StringToPlanNode(type_string));

  json_spirit::Value id_value = find_value(obj, "ID");
  if (id_value == json_spirit::Value::null) {
    delete plan_node;
    throw PlannerException("AbstractPlanNode::fromJSONObject: ID value is null");
  }

  plan_node->plan_node_id = (oid_t) id_value.get_int();
  //VOLT_TRACE("Initializing PlanNode %s", node->debug().c_str());

  json_spirit::Value inlined_nodes_value = find_value(obj,"INLINE_NODES");
  if (inlined_nodes_value == json_spirit::Value::null) {
    delete plan_node;
    throw PlannerException("AbstractPlanNode::fromJSONObject: INLINE_NODES value is null");
  }

  json_spirit::Array inlined_nodes = inlined_nodes_value.get_array();
  for (id_t ii = 0; ii < inlined_nodes.size(); ii++) {
    AbstractPlanNode* new_plan_node = nullptr;
    try {
      json_spirit::Object obj = inlined_nodes[ii].get_obj();
      new_plan_node = AbstractPlanNode::LoadFromJSONObject(obj, catalog_db);
    }
    catch (Exception &ex) {
      delete new_plan_node;
      delete plan_node;
      throw;
    }

    // TODO: if this throws, new Node can be leaked.
    // As long as newNode is not nullptr, this will not throw.
    plan_node->AddInlinePlanNode(new_plan_node);
    //VOLT_TRACE("Adding inline PlanNode %s for %s", new_plan_node->debug().c_str(), plan_node->debug().c_str());
  }

  json_spirit::Value parent_node_ids_value = find_value(obj, "PARENT_IDS");
  if (parent_node_ids_value == json_spirit::Value::null){
    delete plan_node;
    throw PlannerException("AbstractPlanNode::fromJSONObject: PARENT_IDS value is null");
  }

  json_spirit::Array parent_node_ids_array = parent_node_ids_value.get_array();
  for (id_t ii = 0; ii < parent_node_ids_array.size(); ii++) {
    int32_t parentNodeId = (int32_t) parent_node_ids_array[ii].get_int();
    plan_node->parent_ids.push_back(parentNodeId);
  }

  json_spirit::Value child_node_ids_value = find_value(obj, "CHILDREN_IDS");
  if (child_node_ids_value == json_spirit::Value::null) {
    delete plan_node;
    throw PlannerException("AbstractPlanNode::fromJSONObject: CHILDREN_IDS value is null");
  }

  json_spirit::Array child_node_ids_array = child_node_ids_value.get_array();
  for (id_t ii = 0; ii < child_node_ids_array.size(); ii++) {
    int32_t childNodeId = (int32_t) child_node_ids_array[ii].get_int();
    plan_node->children_ids.push_back(childNodeId);
  }

  json_spirit::Value output_columns_value = find_value(obj, "OUTPUT_COLUMNS");
  if (output_columns_value == json_spirit::Value::null) {
    delete plan_node;
    throw PlannerException("AbstractPlanNode::FromJSONObject:"
        " Can't find OUTPUT_COLUMNS value");
  }

  json_spirit::Array output_columns_array = output_columns_value.get_array();
  for (id_t ii = 0; ii < output_columns_array.size(); ii++)  {
    json_spirit::Value outputColumnValue = output_columns_array[ii];
    PlanColumn outputColumn = PlanColumn(outputColumnValue.get_obj());
    plan_node->output_column_guids.push_back(outputColumn.GetGuid());
  }

  try {
    plan_node->LoadFromJSONObject(obj, catalog_db);
  }
  catch (Exception &ex) {
    delete plan_node;
    throw;
  }

  return plan_node;
}

// Get a string representation of this plan node
std::ostream& operator<<(std::ostream& os, const AbstractPlanNode& node) {
  os << node.debug();
  return os;
}

std::string AbstractPlanNode::debug() const {
  std::ostringstream buffer;

  buffer << PlanNodeToString(this->GetPlanNodeType())
      << "[" << this->GetPlanNodeId() << "]";

  return buffer.str();
}

std::string AbstractPlanNode::debug(bool traverse) const {
  return (traverse ? this->debug(std::string("")) : this->debug());
}

std::string AbstractPlanNode::debug(const std::string& spacer) const {
  std::ostringstream buffer;
  buffer << spacer << "* " << this->debug() << "\n";
  std::string info_spacer = spacer + "  |";
  buffer << this->debugInfo(info_spacer);

  // Inline PlanNodes
  if (!inlined_nodes.empty()) {

    buffer << info_spacer << "Inline Plannodes: "
        << inlined_nodes.size() << "\n";
    std::string internal_spacer = info_spacer + "  ";
    for (auto node : inlined_nodes) {
      buffer << info_spacer << "Inline "
          << PlanNodeToString(node.second->GetPlanNodeType())
          << ":\n";
      buffer << node.second->debugInfo(internal_spacer);
    }
  }

  // Traverse the tree
  std::string child_spacer = spacer + "  ";
  for (int ctr = 0, cnt = static_cast<int>(children.size()); ctr < cnt; ctr++) {
    buffer << child_spacer << children[ctr]->GetPlanNodeType() << "\n";
    buffer << children[ctr]->debug(child_spacer);
  }
  return (buffer.str());
}

int AbstractPlanNode::GetColumnIndexFromGuid(int guid,
                                             const catalog::Database* db) const {
  if (children.size() != 1)  {
    return -1;
  }

  AbstractPlanNode* child = children[0];
  if (child == nullptr) {
    return -1;
  }

  return child->GetColumnIndexFromGuid(guid, db);
}

} // namespace planner
} // namespace nstore
