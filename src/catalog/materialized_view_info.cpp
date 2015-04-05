#include "materialized_view_info.h"

#include <cassert>
#include "catalog.h"
#include "column_ref.h"
#include "table.h"

namespace nstore {
namespace catalog {

MaterializedViewInfo::MaterializedViewInfo(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_group_by_cols(catalog, this, path + "/" + "groupbycols"),
  m_dest(nullptr),
  m_vertical_partition(false) {
    CatalogValue value;
    m_fields["dest"] = value;
    m_childCollections["groupbycols"] = &m_group_by_cols;
    m_fields["predicate"] = value;
    m_fields["verticalpartition"] = value;
    m_fields["sqltext"] = value;
}

MaterializedViewInfo::~MaterializedViewInfo() {
    std::map<std::string, ColumnRef*>::const_iterator columnref_iter = m_group_by_cols.begin();
    while (columnref_iter != m_group_by_cols.end()) {
        delete columnref_iter->second;
        columnref_iter++;
    }
    m_group_by_cols.clear();

}

void MaterializedViewInfo::Update() {
    m_dest = m_fields["dest"].typeValue;
    m_predicate = m_fields["predicate"].strValue.c_str();
    m_vertical_partition = m_fields["verticalpartition"].intValue;
    m_sql_text = m_fields["sqltext"].strValue.c_str();
}

CatalogType * MaterializedViewInfo::AddChild(const std::string &collection_name, const std::string &child_name) {
    if (collection_name.compare("groupbycols") == 0) {
        CatalogType *exists = m_group_by_cols.get(child_name);
        if (exists)
            return NULL;
        return m_group_by_cols.add(child_name);
    }
    return NULL;
}

CatalogType * MaterializedViewInfo::GetChild(const std::string &collection_name, const std::string &child_name) const {
    if (collection_name.compare("groupbycols") == 0)
        return m_group_by_cols.get(child_name);
    return NULL;
}

bool MaterializedViewInfo::RemoveChild(const std::string &collection_name, const std::string &child_name) {
    assert (m_childCollections.find(collection_name) != m_childCollections.end());
    if (collection_name.compare("groupbycols") == 0) {
        return m_group_by_cols.remove(child_name);
    }
    return false;
}

const Table * MaterializedViewInfo::GetDestination() const {
    return dynamic_cast<Table*>(m_dest);
}

const CatalogMap<ColumnRef> & MaterializedViewInfo::GetGroupByCols() const {
    return m_group_by_cols;
}

const std::string & MaterializedViewInfo::GetPredicate() const {
    return m_predicate;
}

bool MaterializedViewInfo::IsVerticalPartition() const {
    return m_vertical_partition;
}

const std::string & MaterializedViewInfo::GetSqlText() const {
    return m_sql_text;
}

} // End catalog namespace
} // End nstore namespace
