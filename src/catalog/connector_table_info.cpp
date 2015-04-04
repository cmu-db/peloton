#include "connector_table_info.h"

#include <cassert>
#include "catalog.h"
#include "table.h"

namespace nstore {
namespace catalog {

ConnectorTableInfo::ConnectorTableInfo(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["table"] = value;
    m_fields["appendOnly"] = value;
}

ConnectorTableInfo::~ConnectorTableInfo() {
}

void ConnectorTableInfo::update() {
    m_table = m_fields["table"].typeValue;
    m_appendOnly = m_fields["appendOnly"].intValue;
}

CatalogType * ConnectorTableInfo::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * ConnectorTableInfo::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool ConnectorTableInfo::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

const Table * ConnectorTableInfo::table() const {
    return dynamic_cast<Table*>(m_table);
}

bool ConnectorTableInfo::appendOnly() const {
    return m_appendOnly;
}

} // End catalog namespace
} // End nstore namespace
