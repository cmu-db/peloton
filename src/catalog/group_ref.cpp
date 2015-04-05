#include "group_ref.h"

#include <cassert>
#include "catalog.h"
#include "group.h"

namespace nstore {
namespace catalog {

GroupRef::GroupRef(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_group(nullptr) {
  CatalogValue value;
  m_fields["group"] = value;
}

GroupRef::~GroupRef() {
}

void GroupRef::Update() {
  m_group = m_fields["group"].typeValue;
}

CatalogType * GroupRef::AddChild(__attribute__((unused)) const std::string &collection_name,
                                 __attribute__((unused)) const std::string &child_name) {
  return NULL;
}

CatalogType * GroupRef::GetChild(__attribute__((unused)) const std::string &collection_name,
                                 __attribute__((unused)) const std::string &child_name) const {
  return NULL;
}

bool GroupRef::RemoveChild(const std::string &collection_name,
                           __attribute__((unused)) const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  return false;
}

const Group * GroupRef::GetGroup() const {
  return dynamic_cast<Group*>(m_group);
}

} // End catalog namespace
} // End nstore namespace
