#include <cassert>
#include "procedure.h"

#include "auth_program.h"
#include "catalog.h"
#include "column.h"
#include "conflict_set.h"
#include "group_ref.h"
#include "proc_parameter.h"
#include "statement.h"
#include "table.h"
#include "user_ref.h"

namespace nstore {
namespace catalog {

Procedure::Procedure(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_auth_users(catalog, this, path + "/" + "authUsers"),
  m_auth_groups(catalog, this, path + "/" + "authGroups"),
  m_auth_programs(catalog, this, path + "/" + "authPrograms"),
  m_statements(catalog, this, path + "/" + "statements"),
  m_parameters(catalog, this, path + "/" + "parameters"),
  m_conflicts(catalog, this, path + "/" + "conflicts"),
  m_id(0),
  m_read_only(false),
  m_single_partition(false),
  m_every_site(false),
  m_system_proc(false),
  m_mapreduce(false),
  m_prefetchable(false),
  m_deferrable(false),
  m_has_java(false),
  m_partition_table(nullptr),
  m_partition_column(nullptr),
  m_partition_parameter(0) {
  CatalogValue value;
  m_fields["id"] = value;
  m_fields["classname"] = value;
  m_childCollections["authUsers"] = &m_auth_users;
  m_childCollections["authGroups"] = &m_auth_groups;
  m_fields["readonly"] = value;
  m_fields["singlepartition"] = value;
  m_fields["everysite"] = value;
  m_fields["systemproc"] = value;
  m_fields["mapreduce"] = value;
  m_fields["prefetchable"] = value;
  m_fields["deferrable"] = value;
  m_fields["mapInputQuery"] = value;
  m_fields["mapEmitTable"] = value;
  m_fields["reduceInputQuery"] = value;
  m_fields["reduceEmitTable"] = value;
  m_fields["hasjava"] = value;
  m_fields["partitiontable"] = value;
  m_fields["partitioncolumn"] = value;
  m_fields["partitionparameter"] = value;
  m_childCollections["authPrograms"] = &m_auth_programs;
  m_childCollections["statements"] = &m_statements;
  m_childCollections["parameters"] = &m_parameters;
  m_childCollections["conflicts"] = &m_conflicts;
}

Procedure::~Procedure() {
  std::map<std::string, UserRef*>::const_iterator userref_iter = m_auth_users.begin();
  while (userref_iter != m_auth_users.end()) {
    delete userref_iter->second;
    userref_iter++;
  }
  m_auth_users.clear();

  std::map<std::string, GroupRef*>::const_iterator groupref_iter = m_auth_groups.begin();
  while (groupref_iter != m_auth_groups.end()) {
    delete groupref_iter->second;
    groupref_iter++;
  }
  m_auth_groups.clear();

  std::map<std::string, AuthProgram*>::const_iterator authprogram_iter = m_auth_programs.begin();
  while (authprogram_iter != m_auth_programs.end()) {
    delete authprogram_iter->second;
    authprogram_iter++;
  }
  m_auth_programs.clear();

  std::map<std::string, Statement*>::const_iterator statement_iter = m_statements.begin();
  while (statement_iter != m_statements.end()) {
    delete statement_iter->second;
    statement_iter++;
  }
  m_statements.clear();

  std::map<std::string, ProcParameter*>::const_iterator procparameter_iter = m_parameters.begin();
  while (procparameter_iter != m_parameters.end()) {
    delete procparameter_iter->second;
    procparameter_iter++;
  }
  m_parameters.clear();

  std::map<std::string, ConflictSet*>::const_iterator conflictset_iter = m_conflicts.begin();
  while (conflictset_iter != m_conflicts.end()) {
    delete conflictset_iter->second;
    conflictset_iter++;
  }
  m_conflicts.clear();

}

void Procedure::Update() {
  m_id = m_fields["id"].intValue;
  m_classname = m_fields["classname"].strValue.c_str();
  m_read_only = m_fields["readonly"].intValue;
  m_single_partition = m_fields["singlepartition"].intValue;
  m_every_site = m_fields["everysite"].intValue;
  m_system_proc = m_fields["systemproc"].intValue;
  m_mapreduce = m_fields["mapreduce"].intValue;
  m_prefetchable = m_fields["prefetchable"].intValue;
  m_deferrable = m_fields["deferrable"].intValue;
  m_map_input_query = m_fields["mapInputQuery"].strValue.c_str();
  m_map_emit_table = m_fields["mapEmitTable"].strValue.c_str();
  m_reduce_input_query = m_fields["reduceInputQuery"].strValue.c_str();
  m_reduce_emit_table = m_fields["reduceEmitTable"].strValue.c_str();
  m_has_java = m_fields["hasjava"].intValue;
  m_partition_table = m_fields["partitiontable"].typeValue;
  m_partition_column = m_fields["partitioncolumn"].typeValue;
  m_partition_parameter = m_fields["partitionparameter"].intValue;
}

CatalogType * Procedure::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("authUsers") == 0) {
    CatalogType *exists = m_auth_users.get(child_name);
    if (exists)
      return NULL;
    return m_auth_users.add(child_name);
  }
  if (collection_name.compare("authGroups") == 0) {
    CatalogType *exists = m_auth_groups.get(child_name);
    if (exists)
      return NULL;
    return m_auth_groups.add(child_name);
  }
  if (collection_name.compare("authPrograms") == 0) {
    CatalogType *exists = m_auth_programs.get(child_name);
    if (exists)
      return NULL;
    return m_auth_programs.add(child_name);
  }
  if (collection_name.compare("statements") == 0) {
    CatalogType *exists = m_statements.get(child_name);
    if (exists)
      return NULL;
    return m_statements.add(child_name);
  }
  if (collection_name.compare("parameters") == 0) {
    CatalogType *exists = m_parameters.get(child_name);
    if (exists)
      return NULL;
    return m_parameters.add(child_name);
  }
  if (collection_name.compare("conflicts") == 0) {
    CatalogType *exists = m_conflicts.get(child_name);
    if (exists)
      return NULL;
    return m_conflicts.add(child_name);
  }
  return NULL;
}

CatalogType * Procedure::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("authUsers") == 0)
    return m_auth_users.get(child_name);
  if (collection_name.compare("authGroups") == 0)
    return m_auth_groups.get(child_name);
  if (collection_name.compare("authPrograms") == 0)
    return m_auth_programs.get(child_name);
  if (collection_name.compare("statements") == 0)
    return m_statements.get(child_name);
  if (collection_name.compare("parameters") == 0)
    return m_parameters.get(child_name);
  if (collection_name.compare("conflicts") == 0)
    return m_conflicts.get(child_name);
  return NULL;
}

bool Procedure::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("authUsers") == 0) {
    return m_auth_users.remove(child_name);
  }
  if (collection_name.compare("authGroups") == 0) {
    return m_auth_groups.remove(child_name);
  }
  if (collection_name.compare("authPrograms") == 0) {
    return m_auth_programs.remove(child_name);
  }
  if (collection_name.compare("statements") == 0) {
    return m_statements.remove(child_name);
  }
  if (collection_name.compare("parameters") == 0) {
    return m_parameters.remove(child_name);
  }
  if (collection_name.compare("conflicts") == 0) {
    return m_conflicts.remove(child_name);
  }
  return false;
}

int32_t Procedure::GetId() const {
  return m_id;
}

const std::string & Procedure::GetClassName() const {
  return m_classname;
}

const CatalogMap<UserRef> & Procedure::GetAuthUsers() const {
  return m_auth_users;
}

const CatalogMap<GroupRef> & Procedure::GetAuthGroups() const {
  return m_auth_groups;
}

bool Procedure::IsReadOnly() const {
  return m_read_only;
}

bool Procedure::IsSinglePartition() const {
  return m_single_partition;
}

bool Procedure::IsEverySite() const {
  return m_every_site;
}

bool Procedure::IsSystemProc() const {
  return m_system_proc;
}

bool Procedure::IsMapreduce() const {
  return m_mapreduce;
}

bool Procedure::IsPrefetchable() const {
  return m_prefetchable;
}

bool Procedure::IsDeferrable() const {
  return m_deferrable;
}

const std::string & Procedure::GetMapInputQuery() const {
  return m_map_input_query;
}

const std::string & Procedure::GetMapEmitTable() const {
  return m_map_emit_table;
}

const std::string & Procedure::GetReduceInputQuery() const {
  return m_reduce_input_query;
}

const std::string & Procedure::GetReduceEmitTable() const {
  return m_reduce_emit_table;
}

bool Procedure::IsStoredProcedure() const {
  return m_has_java;
}

const Table * Procedure::GetPartitionTable() const {
  return dynamic_cast<Table*>(m_partition_table);
}

const Column * Procedure::GetPartitionColumn() const {
  return dynamic_cast<Column*>(m_partition_column);
}

int32_t Procedure::GetPartitionParameter() const {
  return m_partition_parameter;
}

const CatalogMap<AuthProgram> & Procedure::GetAuthPrograms() const {
  return m_auth_programs;
}

const CatalogMap<Statement> & Procedure::GetStatements() const {
  return m_statements;
}

const CatalogMap<ProcParameter> & Procedure::GetParameters() const {
  return m_parameters;
}

const CatalogMap<ConflictSet> & Procedure::GetConflicts() const {
  return m_conflicts;
}

} // End catalog namespace
} // End nstore namespace
