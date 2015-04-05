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

public:
    ~ConnectorTableInfo();

    /** GETTER: Reference to the table being ammended */
    const Table *GetTable() const;

    /** GETTER: True if this table is an append-only table for export. */
    bool IsAppendOnly() const;

protected:
    ConnectorTableInfo(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    CatalogType* m_table;

    bool m_append_only;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
