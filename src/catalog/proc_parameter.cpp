#include "proc_parameter.h"

#include <cassert>
#include "catalog.h"

namespace nstore {
namespace catalog {

ProcParameter::ProcParameter(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_type(0),
  m_is_array(false),
  m_index(0) {
    CatalogValue value;
    m_fields["type"] = value;
    m_fields["isarray"] = value;
    m_fields["index"] = value;
}

ProcParameter::~ProcParameter() {
}

void ProcParameter::Update() {
    m_type = m_fields["type"].intValue;
    m_is_array = m_fields["isarray"].intValue;
    m_index = m_fields["index"].intValue;
}

CatalogType * ProcParameter::AddChild(__attribute__((unused)) const std::string &collection_name,
                                      __attribute__((unused)) const std::string &child_name) {
    return NULL;
}

CatalogType * ProcParameter::GetChild(__attribute__((unused)) const std::string &collection_name,
                                      __attribute__((unused)) const std::string &child_name) const {
    return NULL;
}

bool ProcParameter::RemoveChild(const std::string &collection_name,
                                __attribute__((unused)) const std::string &child_name) {
    assert (m_childCollections.find(collection_name) != m_childCollections.end());
    return false;
}

int32_t ProcParameter::GetType() const {
    return m_type;
}

bool ProcParameter::IsArray() const {
    return m_is_array;
}

int32_t ProcParameter::GetIndex() const {
    return m_index;
}

} // End catalog namespace
} // End nstore namespace
