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

 public:
  ~Table();

  /** GETTER: The set of columns in the table */
  const CatalogMap<Column> & GetColumns() const;

  /** GETTER: The set of indexes on the columns in the table */
  const CatalogMap<Index> & GetIndexes() const;

  /** GETTER: The set of constraints on the table */
  const CatalogMap<Constraint> & GetConstraints() const;

  /** GETTER: Is the table replicated? */
  bool IsReplicated() const;

  /** GETTER: On which column is the table horizontally partitioned */
  const Column * GetPartitionColumn() const;

  /** GETTER: A rough estimate of the number of tuples in the table; used for planning */
  int32_t GetEstimatedTupleCount() const;

  /** GETTER: Information about materialized views based on this table's content */
  const CatalogMap<MaterializedViewInfo> & GetViews() const;

  /** GETTER: If this is a materialized view, this field stores the source table */
  const Table *GetMaterializer() const;

  /** GETTER: Is this table an internal system table? */
  bool IsSystable() const;

  /** GETTER: Is this table a MapReduce transaction table? */
  bool IsMapReduce() const;

  /** GETTER: Can contents of this table be evicted by the anti-cache? */
  bool IsEvictable() const;

  /** GETTER: Are contents of this table evicted only along with a parent table and not by itself? */
  bool IsBatchEvicted() const;

 protected:
  Table(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

  CatalogMap<Column> m_columns;

  CatalogMap<Index> m_indexes;

  CatalogMap<Constraint> m_constraints;

  CatalogMap<MaterializedViewInfo> m_views;

  bool m_is_replicated;

  CatalogType* m_partition_column;

  int32_t m_estimated_tuple_count;

  CatalogType* m_materializer;

  bool m_systable;

  bool m_mapreduce;

  bool m_evictable;

  bool m_batch_evicted;

  virtual void Update();

  virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
  virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
  virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

};

} // End catalog namespace
} // End nstore namespace
