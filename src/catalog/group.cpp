#include <cassert>
#include "group.h"
#include "catalog.h"
#include "user_ref.h"

namespace nstore {
namespace catalog {

Group::Group(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_users(catalog, this, path + "/" + "users")
{
    CatalogValue value;
    m_childCollections["users"] = &m_users;
    m_fields["sysproc"] = value;
    m_fields["adhoc"] = value;
}

Group::~Group() {
    std::map<std::string, UserRef*>::const_iterator userref_iter = m_users.begin();
    while (userref_iter != m_users.end()) {
        delete userref_iter->second;
        userref_iter++;
    }
    m_users.clear();

}

void Group::update() {
    m_sysproc = m_fields["sysproc"].intValue;
    m_adhoc = m_fields["adhoc"].intValue;
}

CatalogType * Group::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("users") == 0) {
        CatalogType *exists = m_users.get(childName);
        if (exists)
            return NULL;
        return m_users.add(childName);
    }
    return NULL;
}

CatalogType * Group::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("users") == 0)
        return m_users.get(childName);
    return NULL;
}

bool Group::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("users") == 0) {
        return m_users.remove(childName);
    }
    return false;
}

const CatalogMap<UserRef> & Group::users() const {
    return m_users;
}

bool Group::sysproc() const {
    return m_sysproc;
}

bool Group::adhoc() const {
    return m_adhoc;
}

} // End catalog namespace
} // End nstore namespace
