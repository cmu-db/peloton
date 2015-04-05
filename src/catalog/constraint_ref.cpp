#include "constraint_ref.h"

#include <cassert>
#include "catalog.h"
#include "constraint.h"

namespace nstore {
namespace catalog {

ConstraintRef::ConstraintRef(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_constraint(nullptr) {
  CatalogValue value;
  m_fields["constraint"] = value;
}

ConstraintRef::~ConstraintRef() {
}

void ConstraintRef::Update() {
  m_constraint = m_fields["constraint"].typeValue;
}

CatalogType * ConstraintRef::AddChild(__attribute__((unused)) const std::string &collection_name,
                                      __attribute__((unused)) const std::string &child_name) {
  return NULL;
}

CatalogType * ConstraintRef::GetChild(__attribute__((unused)) const std::string &collection_name,
                                      __attribute__((unused)) const std::string &child_name) const {
  return NULL;
}

bool ConstraintRef::RemoveChild(const std::string &collection_name,
                                __attribute__((unused)) const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  return false;
}

const Constraint * ConstraintRef::GetConstraint() const {
  return dynamic_cast<Constraint*>(m_constraint);
}

} // End catalog namespace
} // End nstore namespace
