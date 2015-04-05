#include <cassert>
#include "database.h"
#include "catalog.h"
#include "user.h"
#include "group.h"
#include "connector.h"
#include "procedure.h"
#include "program.h"
#include "snapshot_schedule.h"
#include "table.h"

namespace nstore {
namespace catalog {

Database::Database(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_users(catalog, this, path + "/" + "users"),
  m_groups(catalog, this, path + "/" + "groups"),
  m_tables(catalog, this, path + "/" + "tables"),
  m_programs(catalog, this, path + "/" + "programs"),
  m_procedures(catalog, this, path + "/" + "procedures"),
  m_connectors(catalog, this, path + "/" + "connectors"),
  m_snapshot_schedule(catalog, this, path + "/" + "snapshotSchedule") {

  CatalogValue value;

  m_fields["project"] = value;
  m_fields["schema"] = value;
  m_childCollections["users"] = &m_users;
  m_childCollections["groups"] = &m_groups;
  m_childCollections["tables"] = &m_tables;
  m_childCollections["programs"] = &m_programs;
  m_childCollections["procedures"] = &m_procedures;
  m_childCollections["connectors"] = &m_connectors;
  m_childCollections["snapshotSchedule"] = &m_snapshot_schedule;
}

Database::~Database() {
  std::map<std::string, User*>::const_iterator user_iter = m_users.begin();
  while (user_iter != m_users.end()) {
    delete user_iter->second;
    user_iter++;
  }
  m_users.clear();

  std::map<std::string, Group*>::const_iterator group_iter = m_groups.begin();
  while (group_iter != m_groups.end()) {
    delete group_iter->second;
    group_iter++;
  }
  m_groups.clear();

  std::map<std::string, Table*>::const_iterator table_iter = m_tables.begin();
  while (table_iter != m_tables.end()) {
    delete table_iter->second;
    table_iter++;
  }
  m_tables.clear();

  std::map<std::string, Program*>::const_iterator program_iter = m_programs.begin();
  while (program_iter != m_programs.end()) {
    delete program_iter->second;
    program_iter++;
  }
  m_programs.clear();

  std::map<std::string, Procedure*>::const_iterator procedure_iter = m_procedures.begin();
  while (procedure_iter != m_procedures.end()) {
    delete procedure_iter->second;
    procedure_iter++;
  }
  m_procedures.clear();

  std::map<std::string, Connector*>::const_iterator connector_iter = m_connectors.begin();
  while (connector_iter != m_connectors.end()) {
    delete connector_iter->second;
    connector_iter++;
  }
  m_connectors.clear();

  std::map<std::string, SnapshotSchedule*>::const_iterator snapshotschedule_iter = m_snapshot_schedule.begin();
  while (snapshotschedule_iter != m_snapshot_schedule.end()) {
    delete snapshotschedule_iter->second;
    snapshotschedule_iter++;
  }
  m_snapshot_schedule.clear();

}

void Database::Update() {
  m_project = m_fields["project"].strValue.c_str();
  m_schema = m_fields["schema"].strValue.c_str();
}

CatalogType * Database::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("users") == 0) {
    CatalogType *exists = m_users.get(child_name);
    if (exists)
      return NULL;
    return m_users.add(child_name);
  }
  if (collection_name.compare("groups") == 0) {
    CatalogType *exists = m_groups.get(child_name);
    if (exists)
      return NULL;
    return m_groups.add(child_name);
  }
  if (collection_name.compare("tables") == 0) {
    CatalogType *exists = m_tables.get(child_name);
    if (exists)
      return NULL;
    return m_tables.add(child_name);
  }
  if (collection_name.compare("programs") == 0) {
    CatalogType *exists = m_programs.get(child_name);
    if (exists)
      return NULL;
    return m_programs.add(child_name);
  }
  if (collection_name.compare("procedures") == 0) {
    CatalogType *exists = m_procedures.get(child_name);
    if (exists)
      return NULL;
    return m_procedures.add(child_name);
  }
  if (collection_name.compare("connectors") == 0) {
    CatalogType *exists = m_connectors.get(child_name);
    if (exists)
      return NULL;
    return m_connectors.add(child_name);
  }
  if (collection_name.compare("snapshotSchedule") == 0) {
    CatalogType *exists = m_snapshot_schedule.get(child_name);
    if (exists)
      return NULL;
    return m_snapshot_schedule.add(child_name);
  }
  return NULL;
}

CatalogType * Database::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("users") == 0)
    return m_users.get(child_name);
  if (collection_name.compare("groups") == 0)
    return m_groups.get(child_name);
  if (collection_name.compare("tables") == 0)
    return m_tables.get(child_name);
  if (collection_name.compare("programs") == 0)
    return m_programs.get(child_name);
  if (collection_name.compare("procedures") == 0)
    return m_procedures.get(child_name);
  if (collection_name.compare("connectors") == 0)
    return m_connectors.get(child_name);
  if (collection_name.compare("snapshotSchedule") == 0)
    return m_snapshot_schedule.get(child_name);
  return NULL;
}

bool Database::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("users") == 0) {
    return m_users.remove(child_name);
  }
  if (collection_name.compare("groups") == 0) {
    return m_groups.remove(child_name);
  }
  if (collection_name.compare("tables") == 0) {
    return m_tables.remove(child_name);
  }
  if (collection_name.compare("programs") == 0) {
    return m_programs.remove(child_name);
  }
  if (collection_name.compare("procedures") == 0) {
    return m_procedures.remove(child_name);
  }
  if (collection_name.compare("connectors") == 0) {
    return m_connectors.remove(child_name);
  }
  if (collection_name.compare("snapshotSchedule") == 0) {
    return m_snapshot_schedule.remove(child_name);
  }
  return false;
}

const std::string & Database::GetProject() const {
  return m_project;
}

const std::string & Database::GetSchema() const {
  return m_schema;
}

const CatalogMap<User> & Database::GetUsers() const {
  return m_users;
}

const CatalogMap<Group> & Database::GetGroups() const {
  return m_groups;
}

const CatalogMap<Table> & Database::GetTables() const {
  return m_tables;
}

const CatalogMap<Program> & Database::GetPrograms() const {
  return m_programs;
}

const CatalogMap<Procedure> & Database::GetProcedures() const {
  return m_procedures;
}

const CatalogMap<Connector> & Database::GetConnectors() const {
  return m_connectors;
}

const CatalogMap<SnapshotSchedule> & Database::GetSnapshotSchedule() const {
  return m_snapshot_schedule;
}

} // End catalog namespace
} // End nstore namespace
