#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// ConnectorTableInfo
//===--------------------------------------------------------------------===//

class Table;
/**
 * Per-export connector table configuration
 */
class ConnectorTableInfo : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<ConnectorTableInfo>;

protected:
    ConnectorTableInfo(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogType* m_table;
    bool m_appendOnly;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~ConnectorTableInfo();

    /** GETTER: Reference to the table being ammended */
    const Table * table() const;
    /** GETTER: True if this table is an append-only table for export. */
    bool appendOnly() const;
};

} // End catalog namespace
} // End nstore namespace
