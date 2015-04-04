#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Host
//===--------------------------------------------------------------------===//

/**
 * A single host participating in the cluster
 */
class Host : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Host>;

protected:
    Host(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_id;
    std::string m_ipaddr;
    int32_t m_num_cpus;
    int32_t m_corespercpu;
    int32_t m_threadspercore;
    int32_t m_memory;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Host();

    /** GETTER: Unique host id */
    int32_t id() const;
    /** GETTER: The ip address or hostname of the host */
    const std::string & ipaddr() const;
    /** GETTER: The max number of cpus on this host */
    int32_t num_cpus() const;
    /** GETTER: The number of cores per CPU on this host */
    int32_t corespercpu() const;
    /** GETTER: The number of threads per cores on this host */
    int32_t threadspercore() const;
    /** GETTER: The amount of memory in bytes that this host has */
    int32_t memory() const;
};

} // End catalog namespace
} // End nstore namespace
