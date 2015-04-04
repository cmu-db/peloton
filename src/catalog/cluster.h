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

protected:
    Cluster(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogMap<Database> m_databases;
    CatalogMap<Host> m_hosts;
    CatalogMap<Site> m_sites;
    int32_t m_num_partitions;
    std::string m_leaderaddress;
    int32_t m_localepoch;
    bool m_securityEnabled;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Cluster();

    /** GETTER: The set of databases the cluster is running */
    const CatalogMap<Database> & databases() const;
    /** GETTER: The set of host that belong to this cluster */
    const CatalogMap<Host> & hosts() const;
    /** GETTER: The set of physical execution contexts executing on this cluster */
    const CatalogMap<Site> & sites() const;
    /** GETTER: The number of partitions in the cluster */
    int32_t num_partitions() const;
    /** GETTER: The ip or hostname of the cluster 'leader' - see docs for details */
    const std::string & leaderaddress() const;
    /** GETTER: The number of seconds since the epoch that we're calling our local epoch */
    int32_t localepoch() const;
    /** GETTER: Whether security and authentication should be enabled/disabled */
    bool securityEnabled() const;
};

} // End catalog namespace
} // End nstore namespace
