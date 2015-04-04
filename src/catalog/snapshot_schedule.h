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

protected:
    SnapshotSchedule(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    std::string m_frequencyUnit;
    int32_t m_frequencyValue;
    int32_t m_retain;
    std::string m_path;
    std::string m_prefix;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~SnapshotSchedule();

    /** GETTER: Unit of time frequency is specified in */
    const std::string & frequencyUnit() const;
    /** GETTER: Frequency in some unit */
    int32_t frequencyValue() const;
    /** GETTER: How many snapshots to retain */
    int32_t retain() const;
    /** GETTER: Path where snapshots should be stored */
    const std::string & path() const;
    /** GETTER: Prefix for snapshot filenames */
    const std::string & prefix() const;
};

} // End catalog namespace
} // End nstore namespace
