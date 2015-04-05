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

public:
    ~Partition();

    /** GETTER: Partition id */
    int32_t GetId() const;

protected:
    Partition(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    int32_t m_id;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
