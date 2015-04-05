#include <cassert>
#include "user.h"
#include "catalog.h"
#include "group_ref.h"

namespace nstore {
namespace catalog {

User::User(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_groups(catalog, this, path + "/" + "groups"),
  m_sysproc(false),
  m_adhoc(false) {
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

void User::Update() {
  m_sysproc = m_fields["sysproc"].intValue;
  m_adhoc = m_fields["adhoc"].intValue;
  m_shadow_password = m_fields["shadowPassword"].strValue.c_str();
}

CatalogType * User::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("groups") == 0) {
    CatalogType *exists = m_groups.get(child_name);
    if (exists)
      return NULL;
    return m_groups.add(child_name);
  }
  return NULL;
}

CatalogType * User::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("groups") == 0)
    return m_groups.get(child_name);
  return NULL;
}

bool User::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("groups") == 0) {
    return m_groups.remove(child_name);
  }
  return false;
}

const CatalogMap<GroupRef> & User::GetGroups() const {
  return m_groups;
}

bool User::CanInvokeSysproc() const {
  return m_sysproc;
}

bool User::CanInvokeAdhoc() const {
  return m_adhoc;
}

const std::string & User::GetShadowPassword() const {
  return m_shadow_password;
}

} // End catalog namespace
} // End nstore namespace
