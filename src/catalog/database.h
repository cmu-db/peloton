#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Database
//===--------------------------------------------------------------------===//

class User;
class Group;
class Table;
class Program;
class Procedure;
class Connector;
class SnapshotSchedule;
/**
 * A set of schema, procedures and other metadata that together comprise an application
 */
class Database : public CatalogType {
    friend class Catalog;
    friend class CatalogMap<Database>;

protected:
    Database(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);
    std::string m_project;
    std::string m_schema;
    CatalogMap<User> m_users;
    CatalogMap<Group> m_groups;
    CatalogMap<Table> m_tables;
    CatalogMap<Program> m_programs;
    CatalogMap<Procedure> m_procedures;
    CatalogMap<Connector> m_connectors;
    CatalogMap<SnapshotSchedule> m_snapshotSchedule;

    virtual void update();

    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);
    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;
    virtual bool removeChild(const std::string &collectionName, const std::string &childName);

public:
    ~Database();

    /** GETTER: The name of the benchmark project used for this database instance. Can be null */
    const std::string & project() const;
    /** GETTER: Full SQL DDL for the database's schema */
    const std::string & schema() const;
    /** GETTER: The set of users */
    const CatalogMap<User> & users() const;
    /** GETTER: The set of groups */
    const CatalogMap<Group> & groups() const;
    /** GETTER: The set of Tables for the database */
    const CatalogMap<Table> & tables() const;
    /** GETTER: The set of programs allowed to access this database */
    const CatalogMap<Program> & programs() const;
    /** GETTER: The set of stored procedures/transactions for this database */
    const CatalogMap<Procedure> & procedures() const;
    /** GETTER: Export connector configuration */
    const CatalogMap<Connector> & connectors() const;
    /** GETTER: Schedule for automated snapshots */
    const CatalogMap<SnapshotSchedule> & snapshotSchedule() const;
};

} // End catalog namespace
} // End nstore namespace
