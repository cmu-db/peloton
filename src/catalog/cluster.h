#pragma once

#include <string>
#include "catalog/catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Cluster
//===--------------------------------------------------------------------===//

class Database;
class Host;
class Site;

/**
 * A set of connected hosts running one or more database application contexts
 */
class Cluster : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Cluster>;

 public:
    ~Cluster();

    /** GETTER: The set of databases the cluster is running */
    const CatalogMap<Database> & GetDatabases() const;

    /** GETTER: The set of host that belong to this cluster */
    const CatalogMap<Host> & GetHosts() const;

    /** GETTER: The set of physical execution contexts executing on this cluster */
    const CatalogMap<Site> & GetSites() const;

    /** GETTER: The number of partitions in the cluster */
    int32_t GetNumPartitions() const;

    /** GETTER: The ip or hostname of the cluster 'leader' - see docs for details */
    const std::string & GetLeaderAddress() const;

    /** GETTER: The number of seconds since the epoch that we're calling our local epoch */
    int32_t GetLocalEpoch() const;

    /** GETTER: Whether security and authentication should be enabled/disabled */
    bool GetSecurityEnabled() const;

protected:
    Cluster(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);

    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;

    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);


    CatalogMap<Database> m_databases;

    CatalogMap<Host> m_hosts;

    CatalogMap<Site> m_sites;

    int32_t m_num_partitions;

    std::string m_leader_address;

    int32_t m_local_epoch;

    bool m_security_enabled;
};

} // End catalog namespace
} // End nstore namespace
