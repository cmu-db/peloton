#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// SnapshotSchedule
//===--------------------------------------------------------------------===//

/**
 * A schedule for the database to follow when creating automated snapshots
 */
class SnapshotSchedule : public CatalogType {
  friend class Catalog;
  friend class CatalogMap<SnapshotSchedule>;

 public:
  ~SnapshotSchedule();

  /** GETTER: Unit of time frequency is specified in */
  const std::string & GetFrequencyUnit() const;

  /** GETTER: Frequency in some unit */
  int32_t GetFrequencyValue() const;

  /** GETTER: How many snapshots to retain */
  int32_t GetRetain() const;

  /** GETTER: Path where snapshots should be stored */
  const std::string & GetPath() const;

  /** GETTER: Prefix for snapshot filenames */
  const std::string & GetPrefix() const;

 protected:
  SnapshotSchedule(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

  std::string m_frequency_unit;

  int32_t m_frequency_value;

  int32_t m_retain;

  std::string m_path;

  std::string m_prefix;

  virtual void Update();

  virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
  virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
  virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);


};

} // End catalog namespace
} // End nstore namespace
