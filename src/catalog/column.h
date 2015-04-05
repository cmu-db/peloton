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

public:
    ~Column();

    /** GETTER: The column's order in the table */
    int32_t GetIndex() const;

    /** GETTER: The type of the column (int/double/date/etc) */
    int32_t GetType() const;

    /** GETTER: (currently unused) */
    int32_t GetSize() const;

    /** GETTER: Is the column nullable? */
    bool IsNullable() const;

    /** GETTER: Default value of the column */
    const std::string & GetDefaultValue() const;

    /** GETTER: Type of the default value of the column */
    int32_t GetDefaultType() const;

    /** GETTER: Constraints that use this column */
    const CatalogMap<ConstraintRef> & GetConstraints() const;

    /** GETTER: If part of a materialized view, ref of view info */
    const MaterializedViewInfo * IsInMatView() const;

    /** GETTER: If part of a materialized view, represents aggregate type */
    int32_t IsAggregateType() const;

    /** GETTER: If part of a materialized view, represents source column */
    const Column * IsMatViewSource() const;

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

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
