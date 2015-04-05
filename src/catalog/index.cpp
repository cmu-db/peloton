#include <cassert>
#include "index.h"
#include "catalog.h"
#include "column_ref.h"

namespace nstore {
namespace catalog {

Index::Index(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_columns(catalog, this, path + "/" + "columns"),
  m_unique(false),
  m_type(0) {
  CatalogValue value;
  m_fields["unique"] = value;
  m_fields["type"] = value;
  m_childCollections["columns"] = &m_columns;
}

Index::~Index() {
  std::map<std::string, ColumnRef*>::const_iterator columnref_iter = m_columns.begin();
  while (columnref_iter != m_columns.end()) {
    delete columnref_iter->second;
    columnref_iter++;
  }
  m_columns.clear();

}

void Index::Update() {
  m_unique = m_fields["unique"].intValue;
  m_type = m_fields["type"].intValue;
}

CatalogType * Index::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("columns") == 0) {
    CatalogType *exists = m_columns.get(child_name);
    if (exists)
      return NULL;
    return m_columns.add(child_name);
  }
  return NULL;
}

CatalogType * Index::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("columns") == 0)
    return m_columns.get(child_name);
  return NULL;
}

bool Index::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("columns") == 0) {
    return m_columns.remove(child_name);
  }
  return false;
}

bool Index::IsUnique() const {
  return m_unique;
}

int32_t Index::GetType() const {
  return m_type;
}

const CatalogMap<ColumnRef> & Index::GetColumns() const {
  return m_columns;
}

} // End catalog namespace
} // End nstore namespace
