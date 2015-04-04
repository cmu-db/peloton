#include "column_ref.h"

#include <cassert>
#include "catalog.h"
#include "column.h"

namespace nstore {
namespace catalog {

ColumnRef::ColumnRef(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["index"] = value;
    m_fields["column"] = value;
}

ColumnRef::~ColumnRef() {
}

void ColumnRef::update() {
    m_index = m_fields["index"].intValue;
    m_column = m_fields["column"].typeValue;
}

CatalogType * ColumnRef::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * ColumnRef::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool ColumnRef::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

int32_t ColumnRef::index() const {
    return m_index;
}

const Column * ColumnRef::column() const {
    return dynamic_cast<Column*>(m_column);
}

} // End catalog namespace
} // End nstore namespace
