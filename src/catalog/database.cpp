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

Database::Database(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_users(catalog, this, path + "/" + "users"), m_groups(catalog, this, path + "/" + "groups"), m_tables(catalog, this, path + "/" + "tables"), m_programs(catalog, this, path + "/" + "programs"), m_procedures(catalog, this, path + "/" + "procedures"), m_connectors(catalog, this, path + "/" + "connectors"), m_snapshotSchedule(catalog, this, path + "/" + "snapshotSchedule")
{
    CatalogValue value;
    m_fields["project"] = value;
    m_fields["schema"] = value;
    m_childCollections["users"] = &m_users;
    m_childCollections["groups"] = &m_groups;
    m_childCollections["tables"] = &m_tables;
    m_childCollections["programs"] = &m_programs;
    m_childCollections["procedures"] = &m_procedures;
    m_childCollections["connectors"] = &m_connectors;
    m_childCollections["snapshotSchedule"] = &m_snapshotSchedule;
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

    std::map<std::string, SnapshotSchedule*>::const_iterator snapshotschedule_iter = m_snapshotSchedule.begin();
    while (snapshotschedule_iter != m_snapshotSchedule.end()) {
        delete snapshotschedule_iter->second;
        snapshotschedule_iter++;
    }
    m_snapshotSchedule.clear();

}

void Database::update() {
    m_project = m_fields["project"].strValue.c_str();
    m_schema = m_fields["schema"].strValue.c_str();
}

CatalogType * Database::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("users") == 0) {
        CatalogType *exists = m_users.get(childName);
        if (exists)
            return NULL;
        return m_users.add(childName);
    }
    if (collectionName.compare("groups") == 0) {
        CatalogType *exists = m_groups.get(childName);
        if (exists)
            return NULL;
        return m_groups.add(childName);
    }
    if (collectionName.compare("tables") == 0) {
        CatalogType *exists = m_tables.get(childName);
        if (exists)
            return NULL;
        return m_tables.add(childName);
    }
    if (collectionName.compare("programs") == 0) {
        CatalogType *exists = m_programs.get(childName);
        if (exists)
            return NULL;
        return m_programs.add(childName);
    }
    if (collectionName.compare("procedures") == 0) {
        CatalogType *exists = m_procedures.get(childName);
        if (exists)
            return NULL;
        return m_procedures.add(childName);
    }
    if (collectionName.compare("connectors") == 0) {
        CatalogType *exists = m_connectors.get(childName);
        if (exists)
            return NULL;
        return m_connectors.add(childName);
    }
    if (collectionName.compare("snapshotSchedule") == 0) {
        CatalogType *exists = m_snapshotSchedule.get(childName);
        if (exists)
            return NULL;
        return m_snapshotSchedule.add(childName);
    }
    return NULL;
}

CatalogType * Database::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("users") == 0)
        return m_users.get(childName);
    if (collectionName.compare("groups") == 0)
        return m_groups.get(childName);
    if (collectionName.compare("tables") == 0)
        return m_tables.get(childName);
    if (collectionName.compare("programs") == 0)
        return m_programs.get(childName);
    if (collectionName.compare("procedures") == 0)
        return m_procedures.get(childName);
    if (collectionName.compare("connectors") == 0)
        return m_connectors.get(childName);
    if (collectionName.compare("snapshotSchedule") == 0)
        return m_snapshotSchedule.get(childName);
    return NULL;
}

bool Database::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("users") == 0) {
        return m_users.remove(childName);
    }
    if (collectionName.compare("groups") == 0) {
        return m_groups.remove(childName);
    }
    if (collectionName.compare("tables") == 0) {
        return m_tables.remove(childName);
    }
    if (collectionName.compare("programs") == 0) {
        return m_programs.remove(childName);
    }
    if (collectionName.compare("procedures") == 0) {
        return m_procedures.remove(childName);
    }
    if (collectionName.compare("connectors") == 0) {
        return m_connectors.remove(childName);
    }
    if (collectionName.compare("snapshotSchedule") == 0) {
        return m_snapshotSchedule.remove(childName);
    }
    return false;
}

const string & Database::project() const {
    return m_project;
}

const string & Database::schema() const {
    return m_schema;
}

const CatalogMap<User> & Database::users() const {
    return m_users;
}

const CatalogMap<Group> & Database::groups() const {
    return m_groups;
}

const CatalogMap<Table> & Database::tables() const {
    return m_tables;
}

const CatalogMap<Program> & Database::programs() const {
    return m_programs;
}

const CatalogMap<Procedure> & Database::procedures() const {
    return m_procedures;
}

const CatalogMap<Connector> & Database::connectors() const {
    return m_connectors;
}

const CatalogMap<SnapshotSchedule> & Database::snapshotSchedule() const {
    return m_snapshotSchedule;
}

} // End catalog namespace
} // End nstore namespace
