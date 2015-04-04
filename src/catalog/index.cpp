#include <cassert>
#include "index.h"
#include "catalog.h"
#include "column_ref.h"

namespace nstore {
namespace catalog {

Index::Index(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_columns(catalog, this, path + "/" + "columns")
{
    CatalogValue value;
    m_fields["unique"] = value;
    m_fields["type"] = value;
    m_childCollections["columns"] = &m_columns;
}

Index::~Index() {
    std::map<std::string, ColumnRef*>::const_iterator columnref_iter = m_columns.begin();
    while (columnref_iter != m_columns.end()) {
        delete columnref_iter->second;
        columnref_iter++;
    }
    m_columns.clear();

}

void Index::update() {
    m_unique = m_fields["unique"].intValue;
    m_type = m_fields["type"].intValue;
}

CatalogType * Index::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("columns") == 0) {
        CatalogType *exists = m_columns.get(childName);
        if (exists)
            return NULL;
        return m_columns.add(childName);
    }
    return NULL;
}

CatalogType * Index::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("columns") == 0)
        return m_columns.get(childName);
    return NULL;
}

bool Index::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("columns") == 0) {
        return m_columns.remove(childName);
    }
    return false;
}

bool Index::unique() const {
    return m_unique;
}

int32_t Index::type() const {
    return m_type;
}

const CatalogMap<ColumnRef> & Index::columns() const {
    return m_columns;
}

} // End catalog namespace
} // End nstore namespace
