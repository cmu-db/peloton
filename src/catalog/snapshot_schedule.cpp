#include "snapshot_schedule.h"

#include <cassert>
#include "catalog.h"

namespace nstore {
namespace catalog {

SnapshotSchedule::SnapshotSchedule(Catalog *catalog, CatalogType *parent, const std::string &path, const std::string &name)
: CatalogType(catalog, parent, path, name),
  m_frequency_value(0),
  m_retain(0) {
  CatalogValue value;
  m_fields["frequencyUnit"] = value;
  m_fields["frequencyValue"] = value;
  m_fields["retain"] = value;
  m_fields["path"] = value;
  m_fields["prefix"] = value;
}

SnapshotSchedule::~SnapshotSchedule() {
}

void SnapshotSchedule::Update() {
  m_frequency_unit = m_fields["frequencyUnit"].strValue.c_str();
  m_frequency_value = m_fields["frequencyValue"].intValue;
  m_retain = m_fields["retain"].intValue;
  m_path = m_fields["path"].strValue.c_str();
  m_prefix = m_fields["prefix"].strValue.c_str();
}

CatalogType * SnapshotSchedule::AddChild(__attribute__((unused)) const std::string &collection_name,
                                         __attribute__((unused)) const std::string &child_name) {
  return NULL;
}

CatalogType * SnapshotSchedule::GetChild(__attribute__((unused)) const std::string &collection_name,
                                         __attribute__((unused)) const std::string &child_name) const {
  return NULL;
}

bool SnapshotSchedule::RemoveChild(const std::string &collection_name,
                                   __attribute__((unused)) const std::string &child_name) {
  assert (m_childCollections.find(collection_name) != m_childCollections.end());
  return false;
}

const std::string & SnapshotSchedule::GetFrequencyUnit() const {
  return m_frequency_unit;
}

int32_t SnapshotSchedule::GetFrequencyValue() const {
  return m_frequency_value;
}

int32_t SnapshotSchedule::GetRetain() const {
  return m_retain;
}

const std::string & SnapshotSchedule::GetPath() const {
  return m_path;
}

const std::string & SnapshotSchedule::GetPrefix() const {
  return m_prefix;
}

} // End catalog namespace
} // End nstore namespace
