#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// CatalogInteger
//===--------------------------------------------------------------------===//

/**
 * Wrapper for an integer so it can be in a list.
 */
class CatalogInteger : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<CatalogInteger>;

protected:
    CatalogInteger(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_value;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool RemoveChild(const std::string &collectionName, const std::string &childName);

public:
    ~CatalogInteger();

    /** GETTER: Integer Value */
    int32_t value() const;
};

} // End catalog namespace
} // End nstore namespace
