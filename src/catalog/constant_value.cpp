#include "constant_value.h"

#include <cassert>
#include "catalog.h"

namespace nstore {
namespace catalog {

ConstantValue::ConstantValue(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["value"] = value;
    m_fields["is_null"] = value;
    m_fields["type"] = value;
}

ConstantValue::~ConstantValue() {
}

void ConstantValue::update() {
    m_value = m_fields["value"].strValue.c_str();
    m_is_null = m_fields["is_null"].intValue;
    m_type = m_fields["type"].intValue;
}

CatalogType * ConstantValue::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * ConstantValue::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool ConstantValue::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

const string & ConstantValue::value() const {
    return m_value;
}

bool ConstantValue::is_null() const {
    return m_is_null;
}

int32_t ConstantValue::type() const {
    return m_type;
}

} // End catalog namespace
} // End nstore namespace
