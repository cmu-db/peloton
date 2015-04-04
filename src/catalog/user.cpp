#include <cassert>
#include "user.h"
#include "catalog.h"
#include "group_ref.h"

namespace nstore {
namespace catalog {

User::User(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_groups(catalog, this, path + "/" + "groups")
{
    CatalogValue value;
    m_childCollections["groups"] = &m_groups;
    m_fields["sysproc"] = value;
    m_fields["adhoc"] = value;
    m_fields["shadowPassword"] = value;
}

User::~User() {
    std::map<std::string, GroupRef*>::const_iterator groupref_iter = m_groups.begin();
    while (groupref_iter != m_groups.end()) {
        delete groupref_iter->second;
        groupref_iter++;
    }
    m_groups.clear();

}

void User::update() {
    m_sysproc = m_fields["sysproc"].intValue;
    m_adhoc = m_fields["adhoc"].intValue;
    m_shadowPassword = m_fields["shadowPassword"].strValue.c_str();
}

CatalogType * User::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("groups") == 0) {
        CatalogType *exists = m_groups.get(childName);
        if (exists)
            return NULL;
        return m_groups.add(childName);
    }
    return NULL;
}

CatalogType * User::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("groups") == 0)
        return m_groups.get(childName);
    return NULL;
}

bool User::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("groups") == 0) {
        return m_groups.remove(childName);
    }
    return false;
}

const CatalogMap<GroupRef> & User::groups() const {
    return m_groups;
}

bool User::sysproc() const {
    return m_sysproc;
}

bool User::adhoc() const {
    return m_adhoc;
}

const string & User::shadowPassword() const {
    return m_shadowPassword;
}

} // End catalog namespace
} // End nstore namespace
