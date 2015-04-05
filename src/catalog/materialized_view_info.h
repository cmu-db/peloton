#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// MaterializedViewInfo
//===--------------------------------------------------------------------===//

class Table;
class ColumnRef;
/**
 * Information used to build and update a materialized view
 */
class MaterializedViewInfo : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<MaterializedViewInfo>;

public:
    ~MaterializedViewInfo();

    /** GETTER: The table which will be updated when the source table is updated */
    const Table * GetDestination() const;

    /** GETTER: The columns involved in the group by of the aggregation */
    const CatalogMap<ColumnRef> & GetGroupByCols() const;

    /** GETTER: A filtering predicate */
    const std::string & GetPredicate() const;

    /** GETTER: Is this materialized view a vertical partition? */
    bool IsVerticalPartition() const;

    /** GETTER: The text of the sql statement for this view */
    const std::string & GetSqlText() const;

protected:
    MaterializedViewInfo(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    CatalogMap<ColumnRef> m_group_by_cols;

    CatalogType* m_dest;

    std::string m_predicate;

    bool m_vertical_partition;

    std::string m_sql_text;

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
