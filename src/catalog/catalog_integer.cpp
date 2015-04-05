#include "catalog_integer.h"

#include <cassert>
#include "catalog.h"

namespace nstore {
namespace catalog {

CatalogInteger::CatalogInteger(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_value(0) {
  CatalogValue value;
  m_fields["value"] = value;
}

CatalogInteger::~CatalogInteger() {
}

void CatalogInteger::Update() {
  m_value = m_fields["value"].intValue;
}

CatalogType * CatalogInteger::AddChild(__attribute__((unused)) const std::string &collection_name,
                                       __attribute__((unused)) const std::string &child_name) {
  return NULL;
}

CatalogType * CatalogInteger::GetChild(__attribute__((unused)) const std::string &collection_name,
                                       __attribute__((unused)) const std::string &child_name) const {
  return NULL;
}

bool CatalogInteger::RemoveChild(const std::string &collection_name,
                                 __attribute__((unused)) const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  return false;
}

int32_t CatalogInteger::GetValue() const {
  return m_value;
}

} // End catalog namespace
} // End nstore namespace
