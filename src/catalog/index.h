#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Index
//===--------------------------------------------------------------------===//

class ColumnRef;
/**
 * A index structure on a database table's columns
 */
class Index : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Index>;

protected:
    Index(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    bool m_unique;
    int32_t m_type;
    CatalogMap<ColumnRef> m_columns;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Index();

    /** GETTER: May the index contain duplicate keys? */
    bool unique() const;
    /** GETTER: What data structure is the index using and what kinds of keys does it support? */
    int32_t type() const;
    /** GETTER: Columns referenced by the index */
    const CatalogMap<ColumnRef> & columns() const;
};

} // End catalog namespace
} // End nstore namespace
