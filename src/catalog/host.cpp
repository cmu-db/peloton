#include <cassert>
#include "host.h"
#include "catalog.h"

namespace nstore {
namespace catalog {

Host::Host(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_id(0),
  m_num_cpus(0),
  m_cores_per_cpu(0),
  m_threads_per_core(0),
  m_memory(0) {
  CatalogValue value;
  m_fields["id"] = value;
  m_fields["ipaddr"] = value;
  m_fields["num_cpus"] = value;
  m_fields["corespercpu"] = value;
  m_fields["threadspercore"] = value;
  m_fields["memory"] = value;
}

Host::~Host() {
}

void Host::Update() {
  m_id = m_fields["id"].intValue;
  m_ipaddr = m_fields["ipaddr"].strValue.c_str();
  m_num_cpus = m_fields["num_cpus"].intValue;
  m_cores_per_cpu = m_fields["corespercpu"].intValue;
  m_threads_per_core = m_fields["threadspercore"].intValue;
  m_memory = m_fields["memory"].intValue;
}

CatalogType * Host::AddChild(__attribute__((unused)) const std::string &collection_name,
                             __attribute__((unused)) const std::string &child_name) {
  return NULL;
}

CatalogType * Host::GetChild(__attribute__((unused)) const std::string &collection_name,
                             __attribute__((unused)) const std::string &child_name) const {
  return NULL;
}

bool Host::RemoveChild(const std::string &collection_name,
                       __attribute__((unused)) const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  return false;
}

int32_t Host::GetId() const {
  return m_id;
}

const std::string & Host::GetIPAddress() const {
  return m_ipaddr;
}

int32_t Host::GetNumCpus() const {
  return m_num_cpus;
}

int32_t Host::GetCoresPerCpu() const {
  return m_cores_per_cpu;
}

int32_t Host::GetThreadsPerCore() const {
  return m_threads_per_core;
}

int32_t Host::GetMemory() const {
  return m_memory;
}

} // End catalog namespace
} // End nstore namespace
