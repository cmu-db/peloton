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

public:
    ~Host();

    /** GETTER: Unique host id */
    int32_t GetId() const;

    /** GETTER: The ip address or hostname of the host */
    const std::string & GetIPAddress() const;

    /** GETTER: The max number of cpus on this host */
    int32_t GetNumCpus() const;

    /** GETTER: The number of cores per CPU on this host */
    int32_t GetCoresPerCpu() const;

    /** GETTER: The number of threads per cores on this host */
    int32_t GetThreadsPerCore() const;

    /** GETTER: The amount of memory in bytes that this host has */
    int32_t GetMemory() const;

protected:
    Host(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    int32_t m_id;

    std::string m_ipaddr;

    int32_t m_num_cpus;

    int32_t m_cores_per_cpu;

    int32_t m_threads_per_core;

    int32_t m_memory;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
