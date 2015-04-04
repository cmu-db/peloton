#include <cassert>
#include "constraint.h"
#include "catalog.h"
#include "column_ref.h"
#include "index.h"
#include "table.h"

namespace nstore {
namespace catalog {

Constraint::Constraint(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_foreignkeycols(catalog, this, path + "/" + "foreignkeycols")
{
    CatalogValue value;
    m_fields["type"] = value;
    m_fields["oncommit"] = value;
    m_fields["index"] = value;
    m_fields["foreignkeytable"] = value;
    m_childCollections["foreignkeycols"] = &m_foreignkeycols;
}

Constraint::~Constraint() {
    std::map<std::string, ColumnRef*>::const_iterator columnref_iter = m_foreignkeycols.begin();
    while (columnref_iter != m_foreignkeycols.end()) {
        delete columnref_iter->second;
        columnref_iter++;
    }
    m_foreignkeycols.clear();

}

void Constraint::update() {
    m_type = m_fields["type"].intValue;
    m_oncommit = m_fields["oncommit"].strValue.c_str();
    m_index = m_fields["index"].typeValue;
    m_foreignkeytable = m_fields["foreignkeytable"].typeValue;
}

CatalogType * Constraint::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("foreignkeycols") == 0) {
        CatalogType *exists = m_foreignkeycols.get(childName);
        if (exists)
            return NULL;
        return m_foreignkeycols.add(childName);
    }
    return NULL;
}

CatalogType * Constraint::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("foreignkeycols") == 0)
        return m_foreignkeycols.get(childName);
    return NULL;
}

bool Constraint::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("foreignkeycols") == 0) {
        return m_foreignkeycols.remove(childName);
    }
    return false;
}

int32_t Constraint::type() const {
    return m_type;
}

const string & Constraint::oncommit() const {
    return m_oncommit;
}

const Index * Constraint::index() const {
    return dynamic_cast<Index*>(m_index);
}

const Table * Constraint::foreignkeytable() const {
    return dynamic_cast<Table*>(m_foreignkeytable);
}

const CatalogMap<ColumnRef> & Constraint::foreignkeycols() const {
    return m_foreignkeycols;
}

} // End catalog namespace
} // End nstore namespace
