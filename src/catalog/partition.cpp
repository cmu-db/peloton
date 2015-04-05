#include <cassert>
#include "partition.h"
#include "catalog.h"

namespace nstore {
namespace catalog {

Partition::Partition(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_id(0) {
    CatalogValue value;
    m_fields["id"] = value;
}

Partition::~Partition() {
}

void Partition::Update() {
    m_id = m_fields["id"].intValue;
}

CatalogType * Partition::AddChild(__attribute__((unused)) const std::string &collection_name,
                                  __attribute__((unused)) const std::string &child_name) {
    return NULL;
}

CatalogType * Partition::GetChild(__attribute__((unused)) const std::string &collection_name,
                                  __attribute__((unused)) const std::string &child_name) const {
    return NULL;
}

bool Partition::RemoveChild(const std::string &collection_name,
                            __attribute__((unused)) const std::string &child_name) {
    assert (m_childCollections.find(collection_name) != m_childCollections.end());
    return false;
}

int32_t Partition::GetId() const {
    return m_id;
}

} // End catalog namespace
} // End nstore namespace
