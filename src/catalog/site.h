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

public:
    ~Site();

    /** GETTER: Site Id */
    int32_t GetId() const;

    /** GETTER: Which host does the site belong to? */
    const Host * GetHost() const;

    /** GETTER: Which logical data partition does this host process? */
    const CatalogMap<Partition> & GetPartitions() const;

    /** GETTER: Is the site up? */
    bool IsUp() const;

    /** GETTER: Port used by HStoreCoordinator */
    int32_t GetMessengerPort() const;

    /** GETTER: Port used by VoltProcedureListener */
    int32_t GetProcPort() const;

protected:
    Site(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    int32_t m_id;

    CatalogType* m_host;

    CatalogMap<Partition> m_partitions;

    bool m_is_up;

    int32_t m_messenger_port;

    int32_t m_proc_port;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
