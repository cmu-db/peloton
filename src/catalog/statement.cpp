#include <cassert>
#include "statement.h"
#include "catalog.h"
#include "column.h"
#include "plan_fragment.h"
#include "stmt_parameter.h"

namespace nstore {
namespace catalog {

Statement::Statement(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_parameters(catalog, this, path + "/" + "parameters"),
  m_output_columns(catalog, this, path + "/" + "output_columns"),
  m_fragments(catalog, this, path + "/" + "fragments"),
  m_ms_fragments(catalog, this, path + "/" + "ms_fragments"),
  m_id(0),
  m_query_type(0),
  m_read_only(false),
  m_single_partition(false),
  m_replicated_table_dml(false),
  m_replicated_only(false),
  m_batched(false),
  m_secondary_index(false),
  m_prefetchable(false),
  m_deferrable(false),
  m_param_num(0),
  m_has_single_sited(false),
  m_has_multi_sited(false),
  m_cost(0) {
  CatalogValue value;
  m_fields["id"] = value;
  m_fields["sqltext"] = value;
  m_fields["querytype"] = value;
  m_fields["readonly"] = value;
  m_fields["singlepartition"] = value;
  m_fields["replicatedtabledml"] = value;
  m_fields["replicatedonly"] = value;
  m_fields["batched"] = value;
  m_fields["secondaryindex"] = value;
  m_fields["prefetchable"] = value;
  m_fields["deferrable"] = value;
  m_fields["paramnum"] = value;
  m_childCollections["parameters"] = &m_parameters;
  m_childCollections["output_columns"] = &m_output_columns;
  m_fields["has_singlesited"] = value;
  m_childCollections["fragments"] = &m_fragments;
  m_fields["exptree"] = value;
  m_fields["fullplan"] = value;
  m_fields["has_multisited"] = value;
  m_childCollections["ms_fragments"] = &m_ms_fragments;
  m_fields["ms_exptree"] = value;
  m_fields["ms_fullplan"] = value;
  m_fields["cost"] = value;
}

Statement::~Statement() {
  std::map<std::string, StmtParameter*>::const_iterator stmtparameter_iter = m_parameters.begin();
  while (stmtparameter_iter != m_parameters.end()) {
    delete stmtparameter_iter->second;
    stmtparameter_iter++;
  }
  m_parameters.clear();

  std::map<std::string, Column*>::const_iterator column_iter = m_output_columns.begin();
  while (column_iter != m_output_columns.end()) {
    delete column_iter->second;
    column_iter++;
  }
  m_output_columns.clear();

  std::map<std::string, PlanFragment*>::const_iterator planfragment_iter = m_fragments.begin();
  while (planfragment_iter != m_fragments.end()) {
    delete planfragment_iter->second;
    planfragment_iter++;
  }
  m_fragments.clear();

  planfragment_iter = m_ms_fragments.begin();
  while (planfragment_iter != m_ms_fragments.end()) {
    delete planfragment_iter->second;
    planfragment_iter++;
  }
  m_ms_fragments.clear();

}

void Statement::Update() {
  m_id = m_fields["id"].intValue;
  m_sql_text = m_fields["sqltext"].strValue.c_str();
  m_query_type = m_fields["querytype"].intValue;
  m_read_only = m_fields["readonly"].intValue;
  m_single_partition = m_fields["singlepartition"].intValue;
  m_replicated_table_dml = m_fields["replicatedtabledml"].intValue;
  m_replicated_only = m_fields["replicatedonly"].intValue;
  m_batched = m_fields["batched"].intValue;
  m_secondary_index = m_fields["secondaryindex"].intValue;
  m_prefetchable = m_fields["prefetchable"].intValue;
  m_deferrable = m_fields["deferrable"].intValue;
  m_param_num = m_fields["paramnum"].intValue;
  m_has_single_sited = m_fields["has_singlesited"].intValue;
  m_exp_tree = m_fields["exptree"].strValue.c_str();
  m_full_plan = m_fields["fullplan"].strValue.c_str();
  m_has_multi_sited = m_fields["has_multisited"].intValue;
  m_ms_exptree = m_fields["ms_exptree"].strValue.c_str();
  m_ms_fullplan = m_fields["ms_fullplan"].strValue.c_str();
  m_cost = m_fields["cost"].intValue;
}

CatalogType * Statement::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("parameters") == 0) {
    CatalogType *exists = m_parameters.get(child_name);
    if (exists)
      return NULL;
    return m_parameters.add(child_name);
  }
  if (collection_name.compare("output_columns") == 0) {
    CatalogType *exists = m_output_columns.get(child_name);
    if (exists)
      return NULL;
    return m_output_columns.add(child_name);
  }
  if (collection_name.compare("fragments") == 0) {
    CatalogType *exists = m_fragments.get(child_name);
    if (exists)
      return NULL;
    return m_fragments.add(child_name);
  }
  if (collection_name.compare("ms_fragments") == 0) {
    CatalogType *exists = m_ms_fragments.get(child_name);
    if (exists)
      return NULL;
    return m_ms_fragments.add(child_name);
  }
  return NULL;
}

CatalogType * Statement::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("parameters") == 0)
    return m_parameters.get(child_name);
  if (collection_name.compare("output_columns") == 0)
    return m_output_columns.get(child_name);
  if (collection_name.compare("fragments") == 0)
    return m_fragments.get(child_name);
  if (collection_name.compare("ms_fragments") == 0)
    return m_ms_fragments.get(child_name);
  return NULL;
}

bool Statement::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("parameters") == 0) {
    return m_parameters.remove(child_name);
  }
  if (collection_name.compare("output_columns") == 0) {
    return m_output_columns.remove(child_name);
  }
  if (collection_name.compare("fragments") == 0) {
    return m_fragments.remove(child_name);
  }
  if (collection_name.compare("ms_fragments") == 0) {
    return m_ms_fragments.remove(child_name);
  }
  return false;
}

int32_t Statement::GetId() const {
  return m_id;
}

const std::string & Statement::GetSqlText() const {
  return m_sql_text;
}

int32_t Statement::GetQueryType() const {
  return m_query_type;
}

bool Statement::IsReadOnly() const {
  return m_read_only;
}

bool Statement::IsSinglePartition() const {
  return m_single_partition;
}

bool Statement::IsReplicatedTableDML() const {
  return m_replicated_table_dml;
}

bool Statement::IsReplicatedOnly() const {
  return m_replicated_only;
}

bool Statement::IsBatched() const {
  return m_batched;
}

bool Statement::UsesSecondaryIndex() const {
  return m_secondary_index;
}

bool Statement::IsPrefetchable() const {
  return m_prefetchable;
}

bool Statement::IsDeferrable() const {
  return m_deferrable;
}

int32_t Statement::GetParamNum() const {
  return m_param_num;
}

const CatalogMap<StmtParameter> & Statement::GetParameters() const {
  return m_parameters;
}

const CatalogMap<Column> & Statement::GetOutputColumns() const {
  return m_output_columns;
}

bool Statement::HasSingleSitedPlan() const {
  return m_has_single_sited;
}

const CatalogMap<PlanFragment> & Statement::GetFragments() const {
  return m_fragments;
}

const std::string & Statement::GetExpressionTree() const {
  return m_exp_tree;
}

const std::string & Statement::GetFullPlan() const {
  return m_full_plan;
}

bool Statement::HasMultiSitedPlan() const {
  return m_has_multi_sited;
}

const CatalogMap<PlanFragment> & Statement::GetMSFragments() const {
  return m_ms_fragments;
}

const std::string & Statement::GetMSExpressionTree() const {
  return m_ms_exptree;
}

const std::string & Statement::GetMSFullPlan() const {
  return m_ms_fullplan;
}

int32_t Statement::cost() const {
  return m_cost;
}

} // End catalog namespace
} // End nstore namespace
