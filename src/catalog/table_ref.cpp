#include "table_ref.h"

#include <cassert>
#include "catalog.h"
#include "table.h"

namespace nstore {
namespace catalog {

TableRef::TableRef(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_table(nullptr) {
  CatalogValue value;
  m_fields["table"] = value;
}

TableRef::~TableRef() {
}

void TableRef::Update() {
  m_table = m_fields["table"].typeValue;
}

CatalogType * TableRef::AddChild(__attribute__((unused)) const std::string &collection_name,
                                 __attribute__((unused)) const std::string &child_name) {
  return NULL;
}

CatalogType * TableRef::GetChild(__attribute__((unused)) const std::string &collection_name,
                                 __attribute__((unused)) const std::string &child_name) const {
  return NULL;
}

bool TableRef::RemoveChild(const std::string &collection_name,
                           __attribute__((unused)) const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  return false;
}

const Table * TableRef::GetTable() const {
  return dynamic_cast<Table*>(m_table);
}

} // End catalog namespace
} // End nstore namespace
