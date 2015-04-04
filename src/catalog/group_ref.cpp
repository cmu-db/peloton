#include "group_ref.h"

#include <cassert>
#include "catalog.h"
#include "group.h"

namespace nstore {
namespace catalog {

GroupRef::GroupRef(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["group"] = value;
}

GroupRef::~GroupRef() {
}

void GroupRef::update() {
    m_group = m_fields["group"].typeValue;
}

CatalogType * GroupRef::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * GroupRef::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool GroupRef::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

const Group * GroupRef::group() const {
    return dynamic_cast<Group*>(m_group);
}

} // End catalog namespace
} // End nstore namespace
