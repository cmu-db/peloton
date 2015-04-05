#include <cassert>
#include "connector.h"
#include "catalog.h"
#include "connector_table_info.h"
#include "group_ref.h"
#include "user_ref.h"

namespace nstore {
namespace catalog {

Connector::Connector(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_enabled(false),
  m_auth_users(catalog, this, path + "/" + "authUsers"),
  m_auth_groups(catalog, this, path + "/" + "authGroups"),
  m_table_info(catalog, this, path + "/" + "tableInfo") {
  CatalogValue value;
  m_fields["loaderclass"] = value;
  m_fields["enabled"] = value;
  m_childCollections["authUsers"] = &m_auth_users;
  m_childCollections["authGroups"] = &m_auth_groups;
  m_childCollections["tableInfo"] = &m_table_info;
}

Connector::~Connector() {
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

  std::map<std::string, ConnectorTableInfo*>::const_iterator connectortableinfo_iter = m_table_info.begin();
  while (connectortableinfo_iter != m_table_info.end()) {
    delete connectortableinfo_iter->second;
    connectortableinfo_iter++;
  }
  m_table_info.clear();

}

void Connector::Update() {
  m_loader_class = m_fields["loaderclass"].strValue.c_str();
  m_enabled = m_fields["enabled"].intValue;
}

CatalogType * Connector::AddChild(const std::string &collection_name, const std::string &child_name) {
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
  if (collection_name.compare("tableInfo") == 0) {
    CatalogType *exists = m_table_info.get(child_name);
    if (exists)
      return NULL;
    return m_table_info.add(child_name);
  }
  return NULL;
}

CatalogType * Connector::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("authUsers") == 0)
    return m_auth_users.get(child_name);
  if (collection_name.compare("authGroups") == 0)
    return m_auth_groups.get(child_name);
  if (collection_name.compare("tableInfo") == 0)
    return m_table_info.get(child_name);
  return NULL;
}

bool Connector::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("authUsers") == 0) {
    return m_auth_users.remove(child_name);
  }
  if (collection_name.compare("authGroups") == 0) {
    return m_auth_groups.remove(child_name);
  }
  if (collection_name.compare("tableInfo") == 0) {
    return m_table_info.remove(child_name);
  }
  return false;
}

const std::string & Connector::GetLoaderClass() const {
  return m_loader_class;
}

bool Connector::IsEnabled() const {
  return m_enabled;
}

const CatalogMap<UserRef> & Connector::GetAuthUsers() const {
  return m_auth_users;
}

const CatalogMap<GroupRef> & Connector::GetAuthGroups() const {
  return m_auth_groups;
}

const CatalogMap<ConnectorTableInfo> & Connector::GetTableInfo() const {
  return m_table_info;
}

} // End catalog namespace
} // End nstore namespace
