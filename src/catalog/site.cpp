#include <cassert>
#include "site.h"
#include "catalog.h"
#include "host.h"
#include "partition.h"

namespace nstore {
namespace catalog {

Site::Site(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_id(0),
  m_host(nullptr),
  m_partitions(catalog, this, path + "/" + "partitions"),
  m_is_up(false),
  m_messenger_port(0),
  m_proc_port(0) {
  CatalogValue value;
  m_fields["id"] = value;
  m_fields["host"] = value;
  m_childCollections["partitions"] = &m_partitions;
  m_fields["isUp"] = value;
  m_fields["messenger_port"] = value;
  m_fields["proc_port"] = value;
}

Site::~Site() {
  std::map<std::string, Partition*>::const_iterator partition_iter = m_partitions.begin();
  while (partition_iter != m_partitions.end()) {
    delete partition_iter->second;
    partition_iter++;
  }
  m_partitions.clear();

}

void Site::Update() {
  m_id = m_fields["id"].intValue;
  m_host = m_fields["host"].typeValue;
  m_is_up = m_fields["isUp"].intValue;
  m_messenger_port = m_fields["messenger_port"].intValue;
  m_proc_port = m_fields["proc_port"].intValue;
}

CatalogType * Site::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("partitions") == 0) {
    CatalogType *exists = m_partitions.get(child_name);
    if (exists)
      return NULL;
    return m_partitions.add(child_name);
  }
  return NULL;
}

CatalogType * Site::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("partitions") == 0)
    return m_partitions.get(child_name);
  return NULL;
}

bool Site::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("partitions") == 0) {
    return m_partitions.remove(child_name);
  }
  return false;
}

int32_t Site::GetId() const {
  return m_id;
}

const Host * Site::GetHost() const {
  return dynamic_cast<Host*>(m_host);
}

const CatalogMap<Partition> & Site::GetPartitions() const {
  return m_partitions;
}

bool Site::IsUp() const {
  return m_is_up;
}

int32_t Site::GetMessengerPort() const {
  return m_messenger_port;
}

int32_t Site::GetProcPort() const {
  return m_proc_port;
}

} // End catalog namespace
} // End nstore namespace
