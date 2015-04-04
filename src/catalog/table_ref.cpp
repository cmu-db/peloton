#include "table_ref.h"

#include <cassert>
#include "catalog.h"
#include "table.h"

namespace nstore {
namespace catalog {

TableRef::TableRef(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["table"] = value;
}

TableRef::~TableRef() {
}

void TableRef::update() {
    m_table = m_fields["table"].typeValue;
}

CatalogType * TableRef::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * TableRef::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool TableRef::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

const Table * TableRef::table() const {
    return dynamic_cast<Table*>(m_table);
}

} // End catalog namespace
} // End nstore namespace
