#include <cassert>
#include "group.h"
#include "catalog.h"
#include "user_ref.h"

namespace nstore {
namespace catalog {

Group::Group(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_users(catalog, this, path + "/" + "users"),
  m_sysproc(false),
  m_adhoc(false) {
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

void Group::Update() {
  m_sysproc = m_fields["sysproc"].intValue;
  m_adhoc = m_fields["adhoc"].intValue;
}

CatalogType * Group::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("users") == 0) {
    CatalogType *exists = m_users.get(child_name);
    if (exists)
      return NULL;
    return m_users.add(child_name);
  }
  return NULL;
}

CatalogType * Group::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("users") == 0)
    return m_users.get(child_name);
  return NULL;
}

bool Group::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("users") == 0) {
    return m_users.remove(child_name);
  }
  return false;
}

const CatalogMap<UserRef> & Group::users() const {
  return m_users;
}

bool Group::CanInvokeSysProc() const {
  return m_sysproc;
}

bool Group::CanInvokeAdhoc() const {
  return m_adhoc;
}

} // End catalog namespace
} // End nstore namespace
