#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Partition
//===--------------------------------------------------------------------===//

/**
 * A logical, replicate-able partition
 */
class Partition : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Partition>;

protected:
    Partition(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_id;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Partition();

    /** GETTER: Partition id */
    int32_t id() const;
};

} // End catalog namespace
} // End nstore namespace
