#include <cassert>
#include "constraint.h"
#include "catalog.h"
#include "column_ref.h"
#include "index.h"
#include "table.h"

namespace nstore {
namespace catalog {

Constraint::Constraint(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_type(0),
  m_index(nullptr),
  m_foreign_key_table(nullptr),
  m_foreign_key_cols(catalog, this, path + "/" + "foreignkeycols") {
  CatalogValue value;
  m_fields["type"] = value;
  m_fields["oncommit"] = value;
  m_fields["index"] = value;
  m_fields["foreignkeytable"] = value;
  m_childCollections["foreignkeycols"] = &m_foreign_key_cols;
}

Constraint::~Constraint() {
  std::map<std::string, ColumnRef*>::const_iterator columnref_iter = m_foreign_key_cols.begin();
  while (columnref_iter != m_foreign_key_cols.end()) {
    delete columnref_iter->second;
    columnref_iter++;
  }
  m_foreign_key_cols.clear();

}

void Constraint::Update() {
  m_type = m_fields["type"].intValue;
  m_on_commit = m_fields["oncommit"].strValue.c_str();
  m_index = m_fields["index"].typeValue;
  m_foreign_key_table = m_fields["foreignkeytable"].typeValue;
}

CatalogType * Constraint::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("foreignkeycols") == 0) {
    CatalogType *exists = m_foreign_key_cols.get(child_name);
    if (exists)
      return NULL;
    return m_foreign_key_cols.add(child_name);
  }
  return NULL;
}

CatalogType * Constraint::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("foreignkeycols") == 0)
    return m_foreign_key_cols.get(child_name);
  return NULL;
}

bool Constraint::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("foreignkeycols") == 0) {
    return m_foreign_key_cols.remove(child_name);
  }
  return false;
}

int32_t Constraint::GetType() const {
  return m_type;
}

const std::string & Constraint::OnCommit() const {
  return m_on_commit;
}

const Index * Constraint::GetIndex() const {
  return dynamic_cast<Index*>(m_index);
}

const Table * Constraint::GetForeignKeyTable() const {
  return dynamic_cast<Table*>(m_foreign_key_table);
}

const CatalogMap<ColumnRef> & Constraint::GetForeignKeyCols() const {
  return m_foreign_key_cols;
}

} // End catalog namespace
} // End nstore namespace
