#include <cassert>
#include "connector.h"
#include "catalog.h"
#include "connector_table_info.h"
#include "group_ref.h"
#include "user_ref.h"

namespace nstore {
namespace catalog {

Connector::Connector(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_authUsers(catalog, this, path + "/" + "authUsers"), m_authGroups(catalog, this, path + "/" + "authGroups"), m_tableInfo(catalog, this, path + "/" + "tableInfo")
{
    CatalogValue value;
    m_fields["loaderclass"] = value;
    m_fields["enabled"] = value;
    m_childCollections["authUsers"] = &m_authUsers;
    m_childCollections["authGroups"] = &m_authGroups;
    m_childCollections["tableInfo"] = &m_tableInfo;
}

Connector::~Connector() {
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

    std::map<std::string, ConnectorTableInfo*>::const_iterator connectortableinfo_iter = m_tableInfo.begin();
    while (connectortableinfo_iter != m_tableInfo.end()) {
        delete connectortableinfo_iter->second;
        connectortableinfo_iter++;
    }
    m_tableInfo.clear();

}

void Connector::update() {
    m_loaderclass = m_fields["loaderclass"].strValue.c_str();
    m_enabled = m_fields["enabled"].intValue;
}

CatalogType * Connector::addChild(const std::string &collectionName, const std::string &childName) {
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
    if (collectionName.compare("tableInfo") == 0) {
        CatalogType *exists = m_tableInfo.get(childName);
        if (exists)
            return NULL;
        return m_tableInfo.add(childName);
    }
    return NULL;
}

CatalogType * Connector::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("authUsers") == 0)
        return m_authUsers.get(childName);
    if (collectionName.compare("authGroups") == 0)
        return m_authGroups.get(childName);
    if (collectionName.compare("tableInfo") == 0)
        return m_tableInfo.get(childName);
    return NULL;
}

bool Connector::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("authUsers") == 0) {
        return m_authUsers.remove(childName);
    }
    if (collectionName.compare("authGroups") == 0) {
        return m_authGroups.remove(childName);
    }
    if (collectionName.compare("tableInfo") == 0) {
        return m_tableInfo.remove(childName);
    }
    return false;
}

const string & Connector::loaderclass() const {
    return m_loaderclass;
}

bool Connector::enabled() const {
    return m_enabled;
}

const CatalogMap<UserRef> & Connector::authUsers() const {
    return m_authUsers;
}

const CatalogMap<GroupRef> & Connector::authGroups() const {
    return m_authGroups;
}

const CatalogMap<ConnectorTableInfo> & Connector::tableInfo() const {
    return m_tableInfo;
}

} // End catalog namespace
} // End nstore namespace
