#include "constraint_ref.h"

#include <cassert>
#include "catalog.h"
#include "constraint.h"

namespace nstore {
namespace catalog {

ConstraintRef::ConstraintRef(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["constraint"] = value;
}

ConstraintRef::~ConstraintRef() {
}

void ConstraintRef::update() {
    m_constraint = m_fields["constraint"].typeValue;
}

CatalogType * ConstraintRef::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * ConstraintRef::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool ConstraintRef::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

const Constraint * ConstraintRef::constraint() const {
    return dynamic_cast<Constraint*>(m_constraint);
}

} // End catalog namespace
} // End nstore namespace
