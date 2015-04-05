#include <cassert>
#include "column.h"
#include "catalog.h"
#include "column.h"

#include "constraint_ref.h"
#include "materialized_view_info.h"

namespace nstore {
namespace catalog {

Column::Column(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_index(0),
  m_type(0),
  m_size(0),
  m_nullable(false),
  m_defaulttype(0),
  m_constraints(catalog, this, path + "/" + "constraints"),
  m_matview(nullptr),
  m_aggregatetype(0),
  m_matviewsource(nullptr) {
  CatalogValue value;
  m_fields["index"] = value;
  m_fields["type"] = value;
  m_fields["size"] = value;
  m_fields["nullable"] = value;
  m_fields["defaultvalue"] = value;
  m_fields["defaulttype"] = value;
  m_childCollections["constraints"] = &m_constraints;
  m_fields["matview"] = value;
  m_fields["aggregatetype"] = value;
  m_fields["matviewsource"] = value;
}

Column::~Column() {
  std::map<std::string, ConstraintRef*>::const_iterator constraintref_iter = m_constraints.begin();
  while (constraintref_iter != m_constraints.end()) {
    delete constraintref_iter->second;
    constraintref_iter++;
  }
  m_constraints.clear();

}

void Column::Update() {
  m_index = m_fields["index"].intValue;
  m_type = m_fields["type"].intValue;
  m_size = m_fields["size"].intValue;
  m_nullable = m_fields["nullable"].intValue;
  m_defaultvalue = m_fields["defaultvalue"].strValue.c_str();
  m_defaulttype = m_fields["defaulttype"].intValue;
  m_matview = m_fields["matview"].typeValue;
  m_aggregatetype = m_fields["aggregatetype"].intValue;
  m_matviewsource = m_fields["matviewsource"].typeValue;
}

CatalogType * Column::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("constraints") == 0) {
    CatalogType *exists = m_constraints.get(child_name);
    if (exists)
      return NULL;
    return m_constraints.add(child_name);
  }
  return NULL;
}

CatalogType * Column::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("constraints") == 0)
    return m_constraints.get(child_name);
  return NULL;
}

bool Column::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("constraints") == 0) {
    return m_constraints.remove(child_name);
  }
  return false;
}

int32_t Column::GetIndex() const {
  return m_index;
}

int32_t Column::GetType() const {
  return m_type;
}

int32_t Column::GetSize() const {
  return m_size;
}

bool Column::IsNullable() const {
  return m_nullable;
}

const std::string & Column::GetDefaultValue() const {
  return m_defaultvalue;
}

int32_t Column::GetDefaultType() const {
  return m_defaulttype;
}

const CatalogMap<ConstraintRef> & Column::GetConstraints() const {
  return m_constraints;
}

const MaterializedViewInfo * Column::IsInMatView() const {
  return dynamic_cast<MaterializedViewInfo*>(m_matview);
}

int32_t Column::IsAggregateType() const {
  return m_aggregatetype;
}

const Column * Column::IsMatViewSource() const {
  return dynamic_cast<Column*>(m_matviewsource);
}

} // End catalog namespace
} // End nstore namespace
