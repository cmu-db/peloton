#include "conflict_set.h"

#include <cassert>
#include "catalog.h"
#include "conflict_pair.h"
#include "procedure.h"

namespace nstore {
namespace catalog {

ConflictSet::ConflictSet(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_procedure(nullptr),
  m_read_write_conflicts(catalog, this, path + "/" + "readWriteConflicts"),
  m_write_write_conflicts(catalog, this, path + "/" + "writeWriteConflicts")
{
    CatalogValue value;
    m_fields["procedure"] = value;
    m_childCollections["readWriteConflicts"] = &m_read_write_conflicts;
    m_childCollections["writeWriteConflicts"] = &m_write_write_conflicts;
}

ConflictSet::~ConflictSet() {
    std::map<std::string, ConflictPair*>::const_iterator conflictpair_iter = m_read_write_conflicts.begin();
    while (conflictpair_iter != m_read_write_conflicts.end()) {
        delete conflictpair_iter->second;
        conflictpair_iter++;
    }
    m_read_write_conflicts.clear();

    conflictpair_iter = m_write_write_conflicts.begin();
    while (conflictpair_iter != m_write_write_conflicts.end()) {
        delete conflictpair_iter->second;
        conflictpair_iter++;
    }
    m_write_write_conflicts.clear();

}

void ConflictSet::Update() {
    m_procedure = m_fields["procedure"].typeValue;
}

CatalogType * ConflictSet::AddChild(const std::string &collection_name, const std::string &child_name) {
    if (collection_name.compare("readWriteConflicts") == 0) {
        CatalogType *exists = m_read_write_conflicts.get(child_name);
        if (exists)
            return NULL;
        return m_read_write_conflicts.add(child_name);
    }
    if (collection_name.compare("writeWriteConflicts") == 0) {
        CatalogType *exists = m_write_write_conflicts.get(child_name);
        if (exists)
            return NULL;
        return m_write_write_conflicts.add(child_name);
    }
    return NULL;
}

CatalogType * ConflictSet::GetChild(const std::string &collection_name, const std::string &child_name) const {
    if (collection_name.compare("readWriteConflicts") == 0)
        return m_read_write_conflicts.get(child_name);
    if (collection_name.compare("writeWriteConflicts") == 0)
        return m_write_write_conflicts.get(child_name);
    return NULL;
}

bool ConflictSet::RemoveChild(const std::string &collection_name, const std::string &child_name) {
    assert (m_childCollections.find(collection_name) != m_childCollections.end());
    if (collection_name.compare("readWriteConflicts") == 0) {
        return m_read_write_conflicts.remove(child_name);
    }
    if (collection_name.compare("writeWriteConflicts") == 0) {
        return m_write_write_conflicts.remove(child_name);
    }
    return false;
}

const Procedure * ConflictSet::GetProcedure() const {
    return dynamic_cast<Procedure*>(m_procedure);
}

const CatalogMap<ConflictPair> & ConflictSet::GetReadWriteConflicts() const {
    return m_read_write_conflicts;
}

const CatalogMap<ConflictPair> & ConflictSet::GetWriteWriteConflicts() const {
    return m_write_write_conflicts;
}

} // End catalog namespace
} // End nstore namespace
