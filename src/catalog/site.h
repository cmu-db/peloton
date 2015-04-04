#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Site
//===--------------------------------------------------------------------===//

class Host;
class Partition;
/**
 * A physical execution context for the system
 */
class Site : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Site>;

protected:
    Site(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_id;
    CatalogType* m_host;
    CatalogMap<Partition> m_partitions;
    bool m_isUp;
    int32_t m_messenger_port;
    int32_t m_proc_port;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Site();

    /** GETTER: Site Id */
    int32_t id() const;
    /** GETTER: Which host does the site belong to? */
    const Host * host() const;
    /** GETTER: Which logical data partition does this host process? */
    const CatalogMap<Partition> & partitions() const;
    /** GETTER: Is the site up? */
    bool isUp() const;
    /** GETTER: Port used by HStoreCoordinator */
    int32_t messenger_port() const;
    /** GETTER: Port used by VoltProcedureListener */
    int32_t proc_port() const;
};

} // End catalog namespace
} // End nstore namespace
