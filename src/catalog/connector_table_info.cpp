#include "connector_table_info.h"

#include <cassert>
#include "catalog.h"
#include "table.h"

namespace nstore {
namespace catalog {

ConnectorTableInfo::ConnectorTableInfo(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_table(nullptr),
  m_append_only(false) {
  CatalogValue value;
  m_fields["table"] = value;
  m_fields["appendOnly"] = value;
}

ConnectorTableInfo::~ConnectorTableInfo() {
}

void ConnectorTableInfo::Update() {
  m_table = m_fields["table"].typeValue;
  m_append_only = m_fields["appendOnly"].intValue;
}

CatalogType * ConnectorTableInfo::AddChild(__attribute__((unused)) const std::string &collection_name,
                                           __attribute__((unused)) const std::string &child_name) {
  return NULL;
}

CatalogType * ConnectorTableInfo::GetChild(__attribute__((unused)) const std::string &collection_name,
                                           __attribute__((unused)) const std::string &child_name) const {
  return NULL;
}

bool ConnectorTableInfo::RemoveChild(const std::string &collection_name,
                                     __attribute__((unused)) const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  return false;
}

const Table * ConnectorTableInfo::GetTable() const {
  return dynamic_cast<Table*>(m_table);
}

bool ConnectorTableInfo::IsAppendOnly() const {
  return m_append_only;
}

} // End catalog namespace
} // End nstore namespace
