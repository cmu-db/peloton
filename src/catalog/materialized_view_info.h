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

protected:
    MaterializedViewInfo(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogType* m_dest;
    CatalogMap<ColumnRef> m_groupbycols;
    std::string m_predicate;
    bool m_verticalpartition;
    std::string m_sqltext;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~MaterializedViewInfo();

    /** GETTER: The table which will be updated when the source table is updated */
    const Table * dest() const;
    /** GETTER: The columns involved in the group by of the aggregation */
    const CatalogMap<ColumnRef> & groupbycols() const;
    /** GETTER: A filtering predicate */
    const std::string & predicate() const;
    /** GETTER: Is this materialized view a vertical partition? */
    bool verticalpartition() const;
    /** GETTER: The text of the sql statement for this view */
    const std::string & sqltext() const;
};

} // End catalog namespace
} // End nstore namespace
