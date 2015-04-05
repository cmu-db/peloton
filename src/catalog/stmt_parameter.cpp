#include "stmt_parameter.h"

#include <cassert>
#include "catalog.h"
#include "proc_parameter.h"

namespace nstore {
namespace catalog {

StmtParameter::StmtParameter(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_sql_type(0),
  m_java_type(0),
  m_index(0),
  m_proc_parameter(nullptr),
  m_proc_parameter_offset(0) {
  CatalogValue value;
  m_fields["sqltype"] = value;
  m_fields["javatype"] = value;
  m_fields["index"] = value;
  m_fields["procparameter"] = value;
  m_fields["procparameteroffset"] = value;
}

StmtParameter::~StmtParameter() {
}

void StmtParameter::Update() {
  m_sql_type = m_fields["sqltype"].intValue;
  m_java_type = m_fields["javatype"].intValue;
  m_index = m_fields["index"].intValue;
  m_proc_parameter = m_fields["procparameter"].typeValue;
  m_proc_parameter_offset = m_fields["procparameteroffset"].intValue;
}

CatalogType * StmtParameter::AddChild(__attribute__((unused)) const std::string &collection_name,
                                      __attribute__((unused)) const std::string &child_name) {
  return NULL;
}

CatalogType * StmtParameter::GetChild(__attribute__((unused)) const std::string &collection_name,
                                      __attribute__((unused)) const std::string &child_name) const {
  return NULL;
}

bool StmtParameter::RemoveChild(const std::string &collection_name,
                                __attribute__((unused)) const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  return false;
}

int32_t StmtParameter::GetSqlType() const {
  return m_sql_type;
}

int32_t StmtParameter::GetJavaType() const {
  return m_java_type;
}

int32_t StmtParameter::GetIndex() const {
  return m_index;
}

const ProcParameter * StmtParameter::GetProcParameter() const {
  return dynamic_cast<ProcParameter*>(m_proc_parameter);
}

int32_t StmtParameter::GetProcParameterOffset() const {
  return m_proc_parameter_offset;
}

} // End catalog namespace
} // End nstore namespace
