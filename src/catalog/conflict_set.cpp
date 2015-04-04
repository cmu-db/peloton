#include "conflict_set.h"

#include <cassert>
#include "catalog.h"
#include "conflict_pair.h"
#include "procedure.h"

namespace nstore {
namespace catalog {

ConflictSet::ConflictSet(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name),
  m_readWriteConflicts(catalog, this, path + "/" + "readWriteConflicts"), m_writeWriteConflicts(catalog, this, path + "/" + "writeWriteConflicts")
{
    CatalogValue value;
    m_fields["procedure"] = value;
    m_childCollections["readWriteConflicts"] = &m_readWriteConflicts;
    m_childCollections["writeWriteConflicts"] = &m_writeWriteConflicts;
}

ConflictSet::~ConflictSet() {
    std::map<std::string, ConflictPair*>::const_iterator conflictpair_iter = m_readWriteConflicts.begin();
    while (conflictpair_iter != m_readWriteConflicts.end()) {
        delete conflictpair_iter->second;
        conflictpair_iter++;
    }
    m_readWriteConflicts.clear();

    conflictpair_iter = m_writeWriteConflicts.begin();
    while (conflictpair_iter != m_writeWriteConflicts.end()) {
        delete conflictpair_iter->second;
        conflictpair_iter++;
    }
    m_writeWriteConflicts.clear();

}

void ConflictSet::update() {
    m_procedure = m_fields["procedure"].typeValue;
}

CatalogType * ConflictSet::addChild(const std::string &collectionName, const std::string &childName) {
    if (collectionName.compare("readWriteConflicts") == 0) {
        CatalogType *exists = m_readWriteConflicts.get(childName);
        if (exists)
            return NULL;
        return m_readWriteConflicts.add(childName);
    }
    if (collectionName.compare("writeWriteConflicts") == 0) {
        CatalogType *exists = m_writeWriteConflicts.get(childName);
        if (exists)
            return NULL;
        return m_writeWriteConflicts.add(childName);
    }
    return NULL;
}

CatalogType * ConflictSet::getChild(const std::string &collectionName, const std::string &childName) const {
    if (collectionName.compare("readWriteConflicts") == 0)
        return m_readWriteConflicts.get(childName);
    if (collectionName.compare("writeWriteConflicts") == 0)
        return m_writeWriteConflicts.get(childName);
    return NULL;
}

bool ConflictSet::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    if (collectionName.compare("readWriteConflicts") == 0) {
        return m_readWriteConflicts.remove(childName);
    }
    if (collectionName.compare("writeWriteConflicts") == 0) {
        return m_writeWriteConflicts.remove(childName);
    }
    return false;
}

const Procedure * ConflictSet::procedure() const {
    return dynamic_cast<Procedure*>(m_procedure);
}

const CatalogMap<ConflictPair> & ConflictSet::readWriteConflicts() const {
    return m_readWriteConflicts;
}

const CatalogMap<ConflictPair> & ConflictSet::writeWriteConflicts() const {
    return m_writeWriteConflicts;
}

} // End catalog namespace
} // End nstore namespace
