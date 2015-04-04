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

Procedure::Procedure(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_authUsers(catalog, this, path + "/" + "authUsers"), m_authGroups(catalog, this, path + "/" + "authGroups"), m_authPrograms(catalog, this, path + "/" + "authPrograms"), m_statements(catalog, this, path + "/" + "statements"), m_parameters(catalog, this, path + "/" + "parameters"), m_conflicts(catalog, this, path + "/" + "conflicts")
{
    CatalogValue value;
    m_fields["id"] = value;
    m_fields["classname"] = value;
    m_childCollections["authUsers"] = &m_authUsers;
    m_childCollections["authGroups"] = &m_authGroups;
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
    m_childCollections["authPrograms"] = &m_authPrograms;
    m_childCollections["statements"] = &m_statements;
    m_childCollections["parameters"] = &m_parameters;
    m_childCollections["conflicts"] = &m_conflicts;
}

Procedure::~Procedure() {
    std::map<std::string, UserRef*>::const_iterator userref_iter = m_authUsers.begin();
    while (userref_iter != m_authUsers.end()) {
        delete userref_iter->second;
        userref_iter++;
    }
    m_authUsers.clear();

    std::map<std::string, GroupRef*>::const_iterator groupref_iter = m_authGroups.begin();
    while (groupref_iter != m_authGroups.end()) {
        delete groupref_iter->second;
        groupref_iter++;
    }
    m_authGroups.clear();

    std::map<std::string, AuthProgram*>::const_iterator authprogram_iter = m_authPrograms.begin();
    while (authprogram_iter != m_authPrograms.end()) {
        delete authprogram_iter->second;
        authprogram_iter++;
    }
    m_authPrograms.clear();

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

void Procedure::update() {
    m_id = m_fields["id"].intValue;
    m_classname = m_fields["classname"].strValue.c_str();
    m_readonly = m_fields["readonly"].intValue;
    m_singlepartition = m_fields["singlepartition"].intValue;
    m_everysite = m_fields["everysite"].intValue;
    m_systemproc = m_fields["systemproc"].intValue;
    m_mapreduce = m_fields["mapreduce"].intValue;
    m_prefetchable = m_fields["prefetchable"].intValue;
    m_deferrable = m_fields["deferrable"].intValue;
    m_mapInputQuery = m_fields["mapInputQuery"].strValue.c_str();
    m_mapEmitTable = m_fields["mapEmitTable"].strValue.c_str();
    m_reduceInputQuery = m_fields["reduceInputQuery"].strValue.c_str();
    m_reduceEmitTable = m_fields["reduceEmitTable"].strValue.c_str();
    m_hasjava = m_fields["hasjava"].intValue;
    m_partitiontable = m_fields["partitiontable"].typeValue;
    m_partitioncolumn = m_fields["partitioncolumn"].typeValue;
    m_partitionparameter = m_fields["partitionparameter"].intValue;
}

CatalogType * Procedure::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("authUsers") == 0) {
        CatalogType *exists = m_authUsers.get(childName);
        if (exists)
            return NULL;
        return m_authUsers.add(childName);
    }
    if (collectionName.compare("authGroups") == 0) {
        CatalogType *exists = m_authGroups.get(childName);
        if (exists)
            return NULL;
        return m_authGroups.add(childName);
    }
    if (collectionName.compare("authPrograms") == 0) {
        CatalogType *exists = m_authPrograms.get(childName);
        if (exists)
            return NULL;
        return m_authPrograms.add(childName);
    }
    if (collectionName.compare("statements") == 0) {
        CatalogType *exists = m_statements.get(childName);
        if (exists)
            return NULL;
        return m_statements.add(childName);
    }
    if (collectionName.compare("parameters") == 0) {
        CatalogType *exists = m_parameters.get(childName);
        if (exists)
            return NULL;
        return m_parameters.add(childName);
    }
    if (collectionName.compare("conflicts") == 0) {
        CatalogType *exists = m_conflicts.get(childName);
        if (exists)
            return NULL;
        return m_conflicts.add(childName);
    }
    return NULL;
}

CatalogType * Procedure::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("authUsers") == 0)
        return m_authUsers.get(childName);
    if (collectionName.compare("authGroups") == 0)
        return m_authGroups.get(childName);
    if (collectionName.compare("authPrograms") == 0)
        return m_authPrograms.get(childName);
    if (collectionName.compare("statements") == 0)
        return m_statements.get(childName);
    if (collectionName.compare("parameters") == 0)
        return m_parameters.get(childName);
    if (collectionName.compare("conflicts") == 0)
        return m_conflicts.get(childName);
    return NULL;
}

bool Procedure::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("authUsers") == 0) {
        return m_authUsers.remove(childName);
    }
    if (collectionName.compare("authGroups") == 0) {
        return m_authGroups.remove(childName);
    }
    if (collectionName.compare("authPrograms") == 0) {
        return m_authPrograms.remove(childName);
    }
    if (collectionName.compare("statements") == 0) {
        return m_statements.remove(childName);
    }
    if (collectionName.compare("parameters") == 0) {
        return m_parameters.remove(childName);
    }
    if (collectionName.compare("conflicts") == 0) {
        return m_conflicts.remove(childName);
    }
    return false;
}

int32_t Procedure::id() const {
    return m_id;
}

const string & Procedure::classname() const {
    return m_classname;
}

const CatalogMap<UserRef> & Procedure::authUsers() const {
    return m_authUsers;
}

const CatalogMap<GroupRef> & Procedure::authGroups() const {
    return m_authGroups;
}

bool Procedure::readonly() const {
    return m_readonly;
}

bool Procedure::singlepartition() const {
    return m_singlepartition;
}

bool Procedure::everysite() const {
    return m_everysite;
}

bool Procedure::systemproc() const {
    return m_systemproc;
}

bool Procedure::mapreduce() const {
    return m_mapreduce;
}

bool Procedure::prefetchable() const {
    return m_prefetchable;
}

bool Procedure::deferrable() const {
    return m_deferrable;
}

const string & Procedure::mapInputQuery() const {
    return m_mapInputQuery;
}

const string & Procedure::mapEmitTable() const {
    return m_mapEmitTable;
}

const string & Procedure::reduceInputQuery() const {
    return m_reduceInputQuery;
}

const string & Procedure::reduceEmitTable() const {
    return m_reduceEmitTable;
}

bool Procedure::hasjava() const {
    return m_hasjava;
}

const Table * Procedure::partitiontable() const {
    return dynamic_cast<Table*>(m_partitiontable);
}

const Column * Procedure::partitioncolumn() const {
    return dynamic_cast<Column*>(m_partitioncolumn);
}

int32_t Procedure::partitionparameter() const {
    return m_partitionparameter;
}

const CatalogMap<AuthProgram> & Procedure::authPrograms() const {
    return m_authPrograms;
}

const CatalogMap<Statement> & Procedure::statements() const {
    return m_statements;
}

const CatalogMap<ProcParameter> & Procedure::parameters() const {
    return m_parameters;
}

const CatalogMap<ConflictSet> & Procedure::conflicts() const {
    return m_conflicts;
}

} // End catalog namespace
} // End nstore namespace
