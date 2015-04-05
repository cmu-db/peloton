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

CatalogType * AuthProgram::AddChild(__attribute__((unused)) const std::string &collection_name,
                                    __attribute__((unused)) const std::string &child_name) {
    return NULL;
}

CatalogType * AuthProgram::GetChild(__attribute__((unused)) const std::string &collection_name,
                                    __attribute__((unused)) const std::string &child_name) const {
    return NULL;
}

bool AuthProgram::RemoveChild(const std::string &collection_name,
                              __attribute__((unused)) const std::string &child_name) {
    assert (m_childCollections.find(collection_name) != m_childCollections.end());
    return false;
}

} // End catalog namespace
} // End nstore namespace
