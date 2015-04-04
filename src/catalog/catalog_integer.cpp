#include "catalog_integer.h"

#include <cassert>
#include "catalog.h"

namespace nstore {
namespace catalog {

CatalogInteger::CatalogInteger(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["value"] = value;
}

CatalogInteger::~CatalogInteger() {
}

void CatalogInteger::Update() {
    m_value = m_fields["value"].intValue;
}

CatalogType * CatalogInteger::AddChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * CatalogInteger::GetChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool CatalogInteger::RemoveChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

int32_t CatalogInteger::value() const {
    return m_value;
}

} // End catalog namespace
} // End nstore namespace
