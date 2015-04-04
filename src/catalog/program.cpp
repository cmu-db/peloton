#include <cassert>
#include "program.h"
#include "catalog.h"

namespace nstore {
namespace catalog {

Program::Program(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
}

Program::~Program() {
}

void Program::update() {
}

CatalogType * Program::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * Program::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool Program::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

} // End catalog namespace
} // End nstore namespace
