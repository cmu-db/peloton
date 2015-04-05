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

public:
    ~Database();

    /** GETTER: The name of the benchmark project used for this database instance. Can be null */
    const std::string & GetProject() const;

    /** GETTER: Full SQL DDL for the database's schema */
    const std::string & GetSchema() const;

    /** GETTER: The set of users */
    const CatalogMap<User> & GetUsers() const;

    /** GETTER: The set of groups */
    const CatalogMap<Group> & GetGroups() const;

    /** GETTER: The set of Tables for the database */
    const CatalogMap<Table> & GetTables() const;

    /** GETTER: The set of programs allowed to access this database */
    const CatalogMap<Program> & GetPrograms() const;

    /** GETTER: The set of stored procedures/transactions for this database */
    const CatalogMap<Procedure> & GetProcedures() const;

    /** GETTER: Export connector configuration */
    const CatalogMap<Connector> & GetConnectors() const;

    /** GETTER: Schedule for automated snapshots */
    const CatalogMap<SnapshotSchedule> & GetSnapshotSchedule() const;

protected:
    Database(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

    virtual void Update();

    virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
    virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
    virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);

    std::string m_project;

    std::string m_schema;

    CatalogMap<User> m_users;

    CatalogMap<Group> m_groups;

    CatalogMap<Table> m_tables;

    CatalogMap<Program> m_programs;

    CatalogMap<Procedure> m_procedures;

    CatalogMap<Connector> m_connectors;

    CatalogMap<SnapshotSchedule> m_snapshot_schedule;

};

} // End catalog namespace
} // End nstore namespace
