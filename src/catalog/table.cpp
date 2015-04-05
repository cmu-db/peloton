#include <cassert>
#include "table.h"
#include "catalog.h"
#include "index.h"
#include "column.h"
#include "constraint.h"
#include "table.h"
#include "materialized_view_info.h"

namespace nstore {
namespace catalog {

Table::Table(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_columns(catalog, this, path + "/" + "columns"),
  m_indexes(catalog, this, path + "/" + "indexes"),
  m_constraints(catalog, this, path + "/" + "constraints"),
  m_views(catalog, this, path + "/" + "views"),
  m_is_replicated(false),
  m_partition_column(nullptr),
  m_estimated_tuple_count(0),
  m_materializer(nullptr),
  m_systable(false),
  m_mapreduce(false),
  m_evictable(false),
  m_batch_evicted(false) {
  CatalogValue value;
  m_childCollections["columns"] = &m_columns;
  m_childCollections["indexes"] = &m_indexes;
  m_childCollections["constraints"] = &m_constraints;
  m_fields["isreplicated"] = value;
  m_fields["partitioncolumn"] = value;
  m_fields["estimatedtuplecount"] = value;
  m_childCollections["views"] = &m_views;
  m_fields["materializer"] = value;
  m_fields["systable"] = value;
  m_fields["mapreduce"] = value;
  m_fields["evictable"] = value;
  m_fields["batchEvicted"] = value;
}

Table::~Table() {
  std::map<std::string, Column*>::const_iterator column_iter = m_columns.begin();
  while (column_iter != m_columns.end()) {
    delete column_iter->second;
    column_iter++;
  }
  m_columns.clear();

  std::map<std::string, Index*>::const_iterator index_iter = m_indexes.begin();
  while (index_iter != m_indexes.end()) {
    delete index_iter->second;
    index_iter++;
  }
  m_indexes.clear();

  std::map<std::string, Constraint*>::const_iterator constraint_iter = m_constraints.begin();
  while (constraint_iter != m_constraints.end()) {
    delete constraint_iter->second;
    constraint_iter++;
  }
  m_constraints.clear();

  std::map<std::string, MaterializedViewInfo*>::const_iterator materializedviewinfo_iter = m_views.begin();
  while (materializedviewinfo_iter != m_views.end()) {
    delete materializedviewinfo_iter->second;
    materializedviewinfo_iter++;
  }
  m_views.clear();

}

void Table::Update() {
  m_is_replicated = m_fields["isreplicated"].intValue;
  m_partition_column = m_fields["partitioncolumn"].typeValue;
  m_estimated_tuple_count = m_fields["estimatedtuplecount"].intValue;
  m_materializer = m_fields["materializer"].typeValue;
  m_systable = m_fields["systable"].intValue;
  m_mapreduce = m_fields["mapreduce"].intValue;
  m_evictable = m_fields["evictable"].intValue;
  m_batch_evicted = m_fields["batchEvicted"].intValue;
}

CatalogType * Table::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("columns") == 0) {
    CatalogType *exists = m_columns.get(child_name);
    if (exists)
      return NULL;
    return m_columns.add(child_name);
  }
  if (collection_name.compare("indexes") == 0) {
    CatalogType *exists = m_indexes.get(child_name);
    if (exists)
      return NULL;
    return m_indexes.add(child_name);
  }
  if (collection_name.compare("constraints") == 0) {
    CatalogType *exists = m_constraints.get(child_name);
    if (exists)
      return NULL;
    return m_constraints.add(child_name);
  }
  if (collection_name.compare("views") == 0) {
    CatalogType *exists = m_views.get(child_name);
    if (exists)
      return NULL;
    return m_views.add(child_name);
  }
  return NULL;
}

CatalogType * Table::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("columns") == 0)
    return m_columns.get(child_name);
  if (collection_name.compare("indexes") == 0)
    return m_indexes.get(child_name);
  if (collection_name.compare("constraints") == 0)
    return m_constraints.get(child_name);
  if (collection_name.compare("views") == 0)
    return m_views.get(child_name);
  return NULL;
}

bool Table::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("columns") == 0) {
    return m_columns.remove(child_name);
  }
  if (collection_name.compare("indexes") == 0) {
    return m_indexes.remove(child_name);
  }
  if (collection_name.compare("constraints") == 0) {
    return m_constraints.remove(child_name);
  }
  if (collection_name.compare("views") == 0) {
    return m_views.remove(child_name);
  }
  return false;
}

const CatalogMap<Column> & Table::GetColumns() const {
  return m_columns;
}

const CatalogMap<Index> & Table::GetIndexes() const {
  return m_indexes;
}

const CatalogMap<Constraint> & Table::GetConstraints() const {
  return m_constraints;
}

bool Table::IsReplicated() const {
  return m_is_replicated;
}

const Column * Table::GetPartitionColumn() const {
  return dynamic_cast<Column*>(m_partition_column);
}

int32_t Table::GetEstimatedTupleCount() const {
  return m_estimated_tuple_count;
}

const CatalogMap<MaterializedViewInfo> & Table::GetViews() const {
  return m_views;
}

const Table * Table::GetMaterializer() const {
  return dynamic_cast<Table*>(m_materializer);
}

bool Table::IsSystable() const {
  return m_systable;
}

bool Table::IsMapReduce() const {
  return m_mapreduce;
}

bool Table::IsEvictable() const {
  return m_evictable;
}

bool Table::IsBatchEvicted() const {
  return m_batch_evicted;
}

} // End catalog namespace
} // End nstore namespace
