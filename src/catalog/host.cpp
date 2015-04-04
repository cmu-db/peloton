#include <cassert>
#include "host.h"
#include "catalog.h"

namespace nstore {
namespace catalog {

Host::Host(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
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

void Host::update() {
    m_id = m_fields["id"].intValue;
    m_ipaddr = m_fields["ipaddr"].strValue.c_str();
    m_num_cpus = m_fields["num_cpus"].intValue;
    m_corespercpu = m_fields["corespercpu"].intValue;
    m_threadspercore = m_fields["threadspercore"].intValue;
    m_memory = m_fields["memory"].intValue;
}

CatalogType * Host::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * Host::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool Host::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

int32_t Host::id() const {
    return m_id;
}

const string & Host::ipaddr() const {
    return m_ipaddr;
}

int32_t Host::num_cpus() const {
    return m_num_cpus;
}

int32_t Host::corespercpu() const {
    return m_corespercpu;
}

int32_t Host::threadspercore() const {
    return m_threadspercore;
}

int32_t Host::memory() const {
    return m_memory;
}

} // End catalog namespace
} // End nstore namespace
