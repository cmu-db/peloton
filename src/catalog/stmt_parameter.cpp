#include "stmt_parameter.h"

#include <cassert>
#include "catalog.h"
#include "proc_parameter.h"

namespace nstore {
namespace catalog {

StmtParameter::StmtParameter(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["sqltype"] = value;
    m_fields["javatype"] = value;
    m_fields["index"] = value;
    m_fields["procparameter"] = value;
    m_fields["procparameteroffset"] = value;
}

StmtParameter::~StmtParameter() {
}

void StmtParameter::update() {
    m_sqltype = m_fields["sqltype"].intValue;
    m_javatype = m_fields["javatype"].intValue;
    m_index = m_fields["index"].intValue;
    m_procparameter = m_fields["procparameter"].typeValue;
    m_procparameteroffset = m_fields["procparameteroffset"].intValue;
}

CatalogType * StmtParameter::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * StmtParameter::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool StmtParameter::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

int32_t StmtParameter::sqltype() const {
    return m_sqltype;
}

int32_t StmtParameter::javatype() const {
    return m_javatype;
}

int32_t StmtParameter::index() const {
    return m_index;
}

const ProcParameter * StmtParameter::procparameter() const {
    return dynamic_cast<ProcParameter*>(m_procparameter);
}

int32_t StmtParameter::procparameteroffset() const {
    return m_procparameteroffset;
}

} // End catalog namespace
} // End nstore namespace
