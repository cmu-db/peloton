#include <cassert>
#include "program.h"
#include "catalog.h"

namespace nstore {
namespace catalog {

Program::Program(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
}

Program::~Program() {
}

void Program::Update() {
}

CatalogType * Program::AddChild(__attribute__((unused)) const std::string &collection_name,
                                __attribute__((unused)) const std::string &child_name) {
    return NULL;
}

CatalogType * Program::GetChild(__attribute__((unused)) const std::string &collection_name,
                                __attribute__((unused)) const std::string &child_name) const {
    return NULL;
}

bool Program::RemoveChild(const std::string &collection_name,
                          __attribute__((unused)) const std::string &child_name) {
    assert (m_childCollections.find(collection_name) != m_childCollections.end());
    return false;
}

} // End catalog namespace
} // End nstore namespace
