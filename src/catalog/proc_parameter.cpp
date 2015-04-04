#include "proc_parameter.h"

#include <cassert>
#include "catalog.h"

namespace nstore {
namespace catalog {

ProcParameter::ProcParameter(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["type"] = value;
    m_fields["isarray"] = value;
    m_fields["index"] = value;
}

ProcParameter::~ProcParameter() {
}

void ProcParameter::update() {
    m_type = m_fields["type"].intValue;
    m_isarray = m_fields["isarray"].intValue;
    m_index = m_fields["index"].intValue;
}

CatalogType * ProcParameter::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * ProcParameter::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool ProcParameter::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

int32_t ProcParameter::type() const {
    return m_type;
}

bool ProcParameter::isarray() const {
    return m_isarray;
}

int32_t ProcParameter::index() const {
    return m_index;
}

} // End catalog namespace
} // End nstore namespace
