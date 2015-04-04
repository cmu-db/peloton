#include "plan_fragment.h"

#include <cassert>
#include "catalog.h"

namespace nstore {
namespace catalog {

PlanFragment::PlanFragment(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["id"] = value;
    m_fields["hasdependencies"] = value;
    m_fields["multipartition"] = value;
    m_fields["readonly"] = value;
    m_fields["plannodetree"] = value;
    m_fields["nontransactional"] = value;
    m_fields["fastaggregate"] = value;
    m_fields["fastcombine"] = value;
}

PlanFragment::~PlanFragment() {
}

void PlanFragment::update() {
    m_id = m_fields["id"].intValue;
    m_hasdependencies = m_fields["hasdependencies"].intValue;
    m_multipartition = m_fields["multipartition"].intValue;
    m_readonly = m_fields["readonly"].intValue;
    m_plannodetree = m_fields["plannodetree"].strValue.c_str();
    m_nontransactional = m_fields["nontransactional"].intValue;
    m_fastaggregate = m_fields["fastaggregate"].intValue;
    m_fastcombine = m_fields["fastcombine"].intValue;
}

CatalogType * PlanFragment::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * PlanFragment::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool PlanFragment::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

int32_t PlanFragment::id() const {
    return m_id;
}

bool PlanFragment::hasdependencies() const {
    return m_hasdependencies;
}

bool PlanFragment::multipartition() const {
    return m_multipartition;
}

bool PlanFragment::readonly() const {
    return m_readonly;
}

const string & PlanFragment::plannodetree() const {
    return m_plannodetree;
}

bool PlanFragment::nontransactional() const {
    return m_nontransactional;
}

bool PlanFragment::fastaggregate() const {
    return m_fastaggregate;
}

bool PlanFragment::fastcombine() const {
    return m_fastcombine;
}

} // End catalog namespace
} // End nstore namespace
