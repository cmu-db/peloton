#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// TableRef
//===--------------------------------------------------------------------===//

class Table;
class TableRef : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<TableRef>;

protected:
    TableRef(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogType* m_table;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~TableRef();

    const Table * table() const;
};

} // End catalog namespace
} // End nstore namespace
