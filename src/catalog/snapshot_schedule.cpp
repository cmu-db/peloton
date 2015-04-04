#include "snapshot_schedule.h"

#include <cassert>
#include "catalog.h"

namespace nstore {
namespace catalog {

SnapshotSchedule::SnapshotSchedule(Catalog *catalog, CatalogType *parent, const string &path, const string &name)
: CatalogType(catalog, parent, path, name)
{
    CatalogValue value;
    m_fields["frequencyUnit"] = value;
    m_fields["frequencyValue"] = value;
    m_fields["retain"] = value;
    m_fields["path"] = value;
    m_fields["prefix"] = value;
}

SnapshotSchedule::~SnapshotSchedule() {
}

void SnapshotSchedule::update() {
    m_frequencyUnit = m_fields["frequencyUnit"].strValue.c_str();
    m_frequencyValue = m_fields["frequencyValue"].intValue;
    m_retain = m_fields["retain"].intValue;
    m_path = m_fields["path"].strValue.c_str();
    m_prefix = m_fields["prefix"].strValue.c_str();
}

CatalogType * SnapshotSchedule::addChild(const std::string &collectionName, const std::string &childName) {
    return NULL;
}

CatalogType * SnapshotSchedule::getChild(const std::string &collectionName, const std::string &childName) const {
    return NULL;
}

bool SnapshotSchedule::removeChild(const std::string &collectionName, const std::string &childName) {
    assert (m_childCollections.find(collectionName) != m_childCollections.end());
    return false;
}

const string & SnapshotSchedule::frequencyUnit() const {
    return m_frequencyUnit;
}

int32_t SnapshotSchedule::frequencyValue() const {
    return m_frequencyValue;
}

int32_t SnapshotSchedule::retain() const {
    return m_retain;
}

const string & SnapshotSchedule::path() const {
    return m_path;
}

const string & SnapshotSchedule::prefix() const {
    return m_prefix;
}

} // End catalog namespace
} // End nstore namespace
