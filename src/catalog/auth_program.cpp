#include "auth_program.h"

#include <cassert>
#include "catalog.h"

namespace nstore {
namespace catalog {

AuthProgram::AuthProgram(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
}

AuthProgram::~AuthProgram() {
}

void AuthProgram::Update() {
}

CatalogType * AuthProgram::AddChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * AuthProgram::GetChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool AuthProgram::RemoveChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

} // End catalog namespace
} // End nstore namespace
