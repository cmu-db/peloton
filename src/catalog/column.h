#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Column
//===--------------------------------------------------------------------===//

class ConstraintRef;
class MaterializedViewInfo;
/**
 * A table column
 */
class Column : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Column>;

protected:
    Column(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    int32_t m_index;
    int32_t m_type;
    int32_t m_size;
    bool m_nullable;
    std::string m_defaultvalue;
    int32_t m_defaulttype;
    CatalogMap<ConstraintRef> m_constraints;
    CatalogType* m_matview;
    int32_t m_aggregatetype;
    CatalogType* m_matviewsource;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Column();

    /** GETTER: The column's order in the table */
    int32_t index() const;
    /** GETTER: The type of the column (int/double/date/etc) */
    int32_t type() const;
    /** GETTER: (currently unused) */
    int32_t size() const;
    /** GETTER: Is the column nullable? */
    bool nullable() const;
    /** GETTER: Default value of the column */
    const std::string & defaultvalue() const;
    /** GETTER: Type of the default value of the column */
    int32_t defaulttype() const;
    /** GETTER: Constraints that use this column */
    const CatalogMap<ConstraintRef> & constraints() const;
    /** GETTER: If part of a materialized view, ref of view info */
    const MaterializedViewInfo * matview() const;
    /** GETTER: If part of a materialized view, represents aggregate type */
    int32_t aggregatetype() const;
    /** GETTER: If part of a materialized view, represents source column */
    const Column * matviewsource() const;
};

} // End catalog namespace
} // End nstore namespace
