#include "conflict_pair.h"

#include <cassert>
#include "catalog.h"
#include "statement.h"
#include "table_ref.h"

namespace nstore {
namespace catalog {

ConflictPair::ConflictPair(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_source_statement(nullptr),
  m_destination_statement(nullptr),
  m_tables(catalog, this, path + "/" + "tables"),
  m_always_conflicting(false),
  m_conflict_type(0) {
  CatalogValue value;
  m_fields["statement0"] = value;
  m_fields["statement1"] = value;
  m_childCollections["tables"] = &m_tables;
  m_fields["alwaysConflicting"] = value;
  m_fields["conflictType"] = value;
}

ConflictPair::~ConflictPair() {
  std::map<std::string, TableRef*>::const_iterator tableref_iter = m_tables.begin();
  while (tableref_iter != m_tables.end()) {
    delete tableref_iter->second;
    tableref_iter++;
  }
  m_tables.clear();

}

void ConflictPair::Update() {
  m_source_statement = m_fields["statement0"].typeValue;
  m_destination_statement = m_fields["statement1"].typeValue;
  m_always_conflicting = m_fields["alwaysConflicting"].intValue;
  m_conflict_type = m_fields["conflictType"].intValue;
}

CatalogType * ConflictPair::AddChild(const std::string &collection_name, const std::string &child_name) {
  if (collection_name.compare("tables") == 0) {
    CatalogType *exists = m_tables.get(child_name);
    if (exists)
      return NULL;
    return m_tables.add(child_name);
  }
  return NULL;
}

CatalogType * ConflictPair::GetChild(const std::string &collection_name, const std::string &child_name) const {
  if (collection_name.compare("tables") == 0)
    return m_tables.get(child_name);
  return NULL;
}

bool ConflictPair::RemoveChild(const std::string &collection_name, const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  if (collection_name.compare("tables") == 0) {
    return m_tables.remove(child_name);
  }
  return false;
}

const Statement * ConflictPair::GetSourceStatement() const {
  return dynamic_cast<Statement*>(m_source_statement);
}

const Statement * ConflictPair::GetDestinationStatement() const {
  return dynamic_cast<Statement*>(m_destination_statement);
}

const CatalogMap<TableRef> & ConflictPair::GetTables() const {
  return m_tables;
}

bool ConflictPair::IsAlwaysConflicting() const {
  return m_always_conflicting;
}

int32_t ConflictPair::GetConflictType() const {
  return m_conflict_type;
}

} // End catalog namespace
} // End nstore namespace
