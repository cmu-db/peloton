#include <cassert>
#include "statement.h"
#include "catalog.h"
#include "column.h"
#include "plan_fragment.h"
#include "stmt_parameter.h"

namespace nstore {
namespace catalog {

Statement::Statement(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_parameters(catalog, this, path + "/" + "parameters"), m_output_columns(catalog, this, path + "/" + "output_columns"), m_fragments(catalog, this, path + "/" + "fragments"), m_ms_fragments(catalog, this, path + "/" + "ms_fragments")
{
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

void Statement::update() {
    m_id = m_fields["id"].intValue;
    m_sqltext = m_fields["sqltext"].strValue.c_str();
    m_querytype = m_fields["querytype"].intValue;
    m_readonly = m_fields["readonly"].intValue;
    m_singlepartition = m_fields["singlepartition"].intValue;
    m_replicatedtabledml = m_fields["replicatedtabledml"].intValue;
    m_replicatedonly = m_fields["replicatedonly"].intValue;
    m_batched = m_fields["batched"].intValue;
    m_secondaryindex = m_fields["secondaryindex"].intValue;
    m_prefetchable = m_fields["prefetchable"].intValue;
    m_deferrable = m_fields["deferrable"].intValue;
    m_paramnum = m_fields["paramnum"].intValue;
    m_has_singlesited = m_fields["has_singlesited"].intValue;
    m_exptree = m_fields["exptree"].strValue.c_str();
    m_fullplan = m_fields["fullplan"].strValue.c_str();
    m_has_multisited = m_fields["has_multisited"].intValue;
    m_ms_exptree = m_fields["ms_exptree"].strValue.c_str();
    m_ms_fullplan = m_fields["ms_fullplan"].strValue.c_str();
    m_cost = m_fields["cost"].intValue;
}

CatalogType * Statement::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("parameters") == 0) {
        CatalogType *exists = m_parameters.get(childName);
        if (exists)
            return NULL;
        return m_parameters.add(childName);
    }
    if (collectionName.compare("output_columns") == 0) {
        CatalogType *exists = m_output_columns.get(childName);
        if (exists)
            return NULL;
        return m_output_columns.add(childName);
    }
    if (collectionName.compare("fragments") == 0) {
        CatalogType *exists = m_fragments.get(childName);
        if (exists)
            return NULL;
        return m_fragments.add(childName);
    }
    if (collectionName.compare("ms_fragments") == 0) {
        CatalogType *exists = m_ms_fragments.get(childName);
        if (exists)
            return NULL;
        return m_ms_fragments.add(childName);
    }
    return NULL;
}

CatalogType * Statement::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("parameters") == 0)
        return m_parameters.get(childName);
    if (collectionName.compare("output_columns") == 0)
        return m_output_columns.get(childName);
    if (collectionName.compare("fragments") == 0)
        return m_fragments.get(childName);
    if (collectionName.compare("ms_fragments") == 0)
        return m_ms_fragments.get(childName);
    return NULL;
}

bool Statement::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("parameters") == 0) {
        return m_parameters.remove(childName);
    }
    if (collectionName.compare("output_columns") == 0) {
        return m_output_columns.remove(childName);
    }
    if (collectionName.compare("fragments") == 0) {
        return m_fragments.remove(childName);
    }
    if (collectionName.compare("ms_fragments") == 0) {
        return m_ms_fragments.remove(childName);
    }
    return false;
}

int32_t Statement::id() const {
    return m_id;
}

const string & Statement::sqltext() const {
    return m_sqltext;
}

int32_t Statement::querytype() const {
    return m_querytype;
}

bool Statement::readonly() const {
    return m_readonly;
}

bool Statement::singlepartition() const {
    return m_singlepartition;
}

bool Statement::replicatedtabledml() const {
    return m_replicatedtabledml;
}

bool Statement::replicatedonly() const {
    return m_replicatedonly;
}

bool Statement::batched() const {
    return m_batched;
}

bool Statement::secondaryindex() const {
    return m_secondaryindex;
}

bool Statement::prefetchable() const {
    return m_prefetchable;
}

bool Statement::deferrable() const {
    return m_deferrable;
}

int32_t Statement::paramnum() const {
    return m_paramnum;
}

const CatalogMap<StmtParameter> & Statement::parameters() const {
    return m_parameters;
}

const CatalogMap<Column> & Statement::output_columns() const {
    return m_output_columns;
}

bool Statement::has_singlesited() const {
    return m_has_singlesited;
}

const CatalogMap<PlanFragment> & Statement::fragments() const {
    return m_fragments;
}

const string & Statement::exptree() const {
    return m_exptree;
}

const string & Statement::fullplan() const {
    return m_fullplan;
}

bool Statement::has_multisited() const {
    return m_has_multisited;
}

const CatalogMap<PlanFragment> & Statement::ms_fragments() const {
    return m_ms_fragments;
}

const string & Statement::ms_exptree() const {
    return m_ms_exptree;
}

const string & Statement::ms_fullplan() const {
    return m_ms_fullplan;
}

int32_t Statement::cost() const {
    return m_cost;
}

} // End catalog namespace
} // End nstore namespace
