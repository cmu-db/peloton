#include "user_ref.h"

#include <cassert>
#include "catalog.h"
#include "user.h"

namespace nstore {
namespace catalog {

UserRef::UserRef(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["user"] = value;
}

UserRef::~UserRef() {
}

void UserRef::update() {
    m_user = m_fields["user"].typeValue;
}

CatalogType * UserRef::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * UserRef::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool UserRef::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

const User * UserRef::user() const {
    return dynamic_cast<User*>(m_user);
}

} // End catalog namespace
} // End nstore namespace
