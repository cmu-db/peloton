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

public:
    ~ColumnRef();

    /** GETTER: The index within the set */
    int32_t GetIndex() const;

    /** GETTER: The table column being referenced */
    const Column * GetColumn() const;

protected:
    ColumnRef(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_index;
    CatalogType* m_column;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
