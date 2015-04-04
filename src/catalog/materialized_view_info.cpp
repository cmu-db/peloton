#include "materialized_view_info.h"

#include <cassert>
#include "catalog.h"
#include "column_ref.h"
#include "table.h"

namespace nstore {
namespace catalog {

MaterializedViewInfo::MaterializedViewInfo(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_groupbycols(catalog, this, path + "/" + "groupbycols")
{
    CatalogValue value;
    m_fields["dest"] = value;
    m_childCollections["groupbycols"] = &m_groupbycols;
    m_fields["predicate"] = value;
    m_fields["verticalpartition"] = value;
    m_fields["sqltext"] = value;
}

MaterializedViewInfo::~MaterializedViewInfo() {
    std::map<std::string, ColumnRef*>::const_iterator columnref_iter = m_groupbycols.begin();
    while (columnref_iter != m_groupbycols.end()) {
        delete columnref_iter->second;
        columnref_iter++;
    }
    m_groupbycols.clear();

}

void MaterializedViewInfo::update() {
    m_dest = m_fields["dest"].typeValue;
    m_predicate = m_fields["predicate"].strValue.c_str();
    m_verticalpartition = m_fields["verticalpartition"].intValue;
    m_sqltext = m_fields["sqltext"].strValue.c_str();
}

CatalogType * MaterializedViewInfo::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("groupbycols") == 0) {
        CatalogType *exists = m_groupbycols.get(childName);
        if (exists)
            return NULL;
        return m_groupbycols.add(childName);
    }
    return NULL;
}

CatalogType * MaterializedViewInfo::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("groupbycols") == 0)
        return m_groupbycols.get(childName);
    return NULL;
}

bool MaterializedViewInfo::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("groupbycols") == 0) {
        return m_groupbycols.remove(childName);
    }
    return false;
}

const Table * MaterializedViewInfo::dest() const {
    return dynamic_cast<Table*>(m_dest);
}

const CatalogMap<ColumnRef> & MaterializedViewInfo::groupbycols() const {
    return m_groupbycols;
}

const string & MaterializedViewInfo::predicate() const {
    return m_predicate;
}

bool MaterializedViewInfo::verticalpartition() const {
    return m_verticalpartition;
}

const string & MaterializedViewInfo::sqltext() const {
    return m_sqltext;
}

} // End catalog namespace
} // End nstore namespace
