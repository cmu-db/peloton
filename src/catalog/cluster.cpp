#include <cassert>
#include "cluster.h"
#include "catalog.h"
#include "database.h"
#include "host.h"
#include "site.h"

namespace nstore {
namespace catalog {

Cluster::Cluster(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_databases(catalog, this, path + "/" + "databases"),
  m_hosts(catalog, this, path + "/" + "hosts"),
  m_sites(catalog, this, path + "/" + "sites"),
  m_num_partitions(0),
  m_local_epoch(0),
  m_security_enabled(false) {

  CatalogValue value;
  m_childCollections["databases"] = &m_databases;
  m_childCollections["hosts"] = &m_hosts;
  m_childCollections["sites"] = &m_sites;
  m_fields["num_partitions"] = value;
  m_fields["leaderaddress"] = value;
  m_fields["localepoch"] = value;
  m_fields["securityEnabled"] = value;

}

Cluster::~Cluster() {
  std::map<std::string, Database*>::const_iterator database_iter = m_databases.begin();
  while (database_iter != m_databases.end()) {
    delete database_iter->second;
    database_iter++;
  }
  m_databases.clear();

  std::map<std::string, Host*>::const_iterator host_iter = m_hosts.begin();
  while (host_iter != m_hosts.end()) {
    delete host_iter->second;
    host_iter++;
  }
  m_hosts.clear();

  std::map<std::string, Site*>::const_iterator site_iter = m_sites.begin();
  while (site_iter != m_sites.end()) {
    delete site_iter->second;
    site_iter++;
  }
  m_sites.clear();

}

void Cluster::Update() {
  m_num_partitions = m_fields["num_partitions"].intValue;
  m_leader_address = m_fields["leaderaddress"].strValue.c_str();
  m_local_epoch = m_fields["localepoch"].intValue;
  m_security_enabled = m_fields["securityEnabled"].intValue;
}

CatalogType * Cluster::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("databases") == 0) {
    CatalogType *exists = m_databases.get(child_name);
    if (exists)
      return NULL;
    return m_databases.add(child_name);
  }
  if (collection_name.compare("hosts") == 0) {
    CatalogType *exists = m_hosts.get(child_name);
    if (exists)
      return NULL;
    return m_hosts.add(child_name);
  }
  if (collection_name.compare("sites") == 0) {
    CatalogType *exists = m_sites.get(child_name);
    if (exists)
      return NULL;
    return m_sites.add(child_name);
  }
  return NULL;
}

CatalogType * Cluster::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("databases") == 0)
    return m_databases.get(child_name);
  if (collection_name.compare("hosts") == 0)
    return m_hosts.get(child_name);
  if (collection_name.compare("sites") == 0)
    return m_sites.get(child_name);
  return NULL;
}

bool Cluster::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("databases") == 0) {
    return m_databases.remove(child_name);
  }
  if (collection_name.compare("hosts") == 0) {
    return m_hosts.remove(child_name);
  }
  if (collection_name.compare("sites") == 0) {
    return m_sites.remove(child_name);
  }
  return false;
}

const CatalogMap<Database> & Cluster::GetDatabases() const {
  return m_databases;
}

const CatalogMap<Host> & Cluster::GetHosts() const {
  return m_hosts;
}

const CatalogMap<Site> & Cluster::GetSites() const {
  return m_sites;
}

int32_t Cluster::GetNumPartitions() const {
  return m_num_partitions;
}

const std::string & Cluster::GetLeaderAddress() const {
  return m_leader_address;
}

int32_t Cluster::GetLocalEpoch() const {
  return m_local_epoch;
}

bool Cluster::GetSecurityEnabled() const {
  return m_security_enabled;
}

} // End catalog namespace
} // End nstore namespace
