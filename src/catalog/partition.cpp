#include <cassert>
#include "partition.h"
#include "catalog.h"

namespace nstore {
namespace catalog {

Partition::Partition(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["id"] = value;
}

Partition::~Partition() {
}

void Partition::update() {
    m_id = m_fields["id"].intValue;
}

CatalogType * Partition::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * Partition::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool Partition::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

int32_t Partition::id() const {
    return m_id;
}

} // End catalog namespace
} // End nstore namespace
