#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Table
//===--------------------------------------------------------------------===//

class Column;
class Index;
class Constraint;
class MaterializedViewInfo;
/**
 * A table (relation) in the database
 */
class Table : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Table>;

protected:
    Table(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    CatalogMap<Column> m_columns;
    CatalogMap<Index> m_indexes;
    CatalogMap<Constraint> m_constraints;
    bool m_isreplicated;
    CatalogType* m_partitioncolumn;
    int32_t m_estimatedtuplecount;
    CatalogMap<MaterializedViewInfo> m_views;
    CatalogType* m_materializer;
    bool m_systable;
    bool m_mapreduce;
    bool m_evictable;
    bool m_batchEvicted;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Table();

    /** GETTER: The set of columns in the table */
    const CatalogMap<Column> & columns() const;
    /** GETTER: The set of indexes on the columns in the table */
    const CatalogMap<Index> & indexes() const;
    /** GETTER: The set of constraints on the table */
    const CatalogMap<Constraint> & constraints() const;
    /** GETTER: Is the table replicated? */
    bool isreplicated() const;
    /** GETTER: On which column is the table horizontally partitioned */
    const Column * partitioncolumn() const;
    /** GETTER: A rough estimate of the number of tuples in the table; used for planning */
    int32_t estimatedtuplecount() const;
    /** GETTER: Information about materialized views based on this table's content */
    const CatalogMap<MaterializedViewInfo> & views() const;
    /** GETTER: If this is a materialized view, this field stores the source table */
    const Table * materializer() const;
    /** GETTER: Is this table an internal system table? */
    bool systable() const;
    /** GETTER: Is this table a MapReduce transaction table? */
    bool mapreduce() const;
    /** GETTER: Can contents of this table be evicted by the anti-cache? */
    bool evictable() const;
    /** GETTER: Are contents of this table evicted only along with a parent table and not by itself? */
    bool batchEvicted() const;
};

} // End catalog namespace
} // End nstore namespace
