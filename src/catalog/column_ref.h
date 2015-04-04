#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// ColumnRef
//===--------------------------------------------------------------------===//

class Column;
/**
 * A reference to a table column
 */
class ColumnRef : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<ColumnRef>;

protected:
    ColumnRef(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_index;
    CatalogType* m_column;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~ColumnRef();

    /** GETTER: The index within the set */
    int32_t index() const;
    /** GETTER: The table column being referenced */
    const Column * column() const;
};

} // End catalog namespace
} // End nstore namespace
