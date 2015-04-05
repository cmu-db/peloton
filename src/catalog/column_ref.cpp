#include "column_ref.h"

#include <cassert>
#include "catalog.h"
#include "column.h"

namespace nstore {
namespace catalog {

ColumnRef::ColumnRef(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_index(0),
  m_column(nullptr) {
  CatalogValue value;
  m_fields["index"] = value;
  m_fields["column"] = value;
}

ColumnRef::~ColumnRef() {
}

void ColumnRef::Update() {
  m_index = m_fields["index"].intValue;
  m_column = m_fields["column"].typeValue;
}

CatalogType * ColumnRef::AddChild(__attribute__((unused)) const std::string &collection_name,
                                  __attribute__((unused)) const std::string &child_name) {
  return NULL;
}

CatalogType * ColumnRef::GetChild(__attribute__((unused)) const std::string &collection_name,
                                  __attribute__((unused)) const std::string &child_name) const {
  return NULL;
}

bool ColumnRef::RemoveChild(const std::string &collection_name, __attribute__((unused)) const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  return false;
}

int32_t ColumnRef::GetIndex() const {
  return m_index;
}

const Column * ColumnRef::GetColumn() const {
  return dynamic_cast<Column*>(m_column);
}

} // End catalog namespace
} // End nstore namespace
