#include "constant_value.h"

#include <cassert>
#include "catalog.h"

namespace nstore {
namespace catalog {

ConstantValue::ConstantValue(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_is_null(false),
  m_type(0) {
    CatalogValue value;
    m_fields["value"] = value;
    m_fields["is_null"] = value;
    m_fields["type"] = value;
}

ConstantValue::~ConstantValue() {
}

void ConstantValue::Update() {
    m_value = m_fields["value"].strValue.c_str();
    m_is_null = m_fields["is_null"].intValue;
    m_type = m_fields["type"].intValue;
}

CatalogType * ConstantValue::AddChild(__attribute__((unused)) const std::string &collection_name,
                                      __attribute__((unused)) const std::string &child_name) {
    return NULL;
}

CatalogType * ConstantValue::GetChild(__attribute__((unused)) const std::string &collection_name,
                                      __attribute__((unused)) const std::string &child_name) const {
    return NULL;
}

bool ConstantValue::RemoveChild(const std::string &collection_name,
                                __attribute__((unused)) const std::string &child_name) {
    assert (m_childCollections.find(collection_name) != m_childCollections.end());
    return false;
}

const std::string & ConstantValue::GetValue() const {
    return m_value;
}

bool ConstantValue::IsNull() const {
    return m_is_null;
}

int32_t ConstantValue::GetType() const {
    return m_type;
}

} // End catalog namespace
} // End nstore namespace
