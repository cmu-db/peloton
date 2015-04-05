#include "user_ref.h"

#include <cassert>
#include "catalog.h"
#include "user.h"

namespace nstore {
namespace catalog {

UserRef::UserRef(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_user(nullptr) {
  CatalogValue value;
  m_fields["user"] = value;
}

UserRef::~UserRef() {
}

void UserRef::Update() {
  m_user = m_fields["user"].typeValue;
}

CatalogType * UserRef::AddChild(__attribute__((unused)) const std::string &collection_name,
                                __attribute__((unused)) const std::string &child_name) {
  return NULL;
}

CatalogType * UserRef::GetChild(__attribute__((unused)) const std::string &collection_name,
                                __attribute__((unused)) const std::string &child_name) const {
  return NULL;
}

bool UserRef::RemoveChild(const std::string &collection_name,
                          __attribute__((unused)) const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  return false;
}

const User * UserRef::GetUser() const {
  return dynamic_cast<User*>(m_user);
}

} // End catalog namespace
} // End nstore namespace
