#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Procedure
//===--------------------------------------------------------------------===//

class UserRef;
class GroupRef;
class Table;
class Column;
class AuthProgram;
class Statement;
class ProcParameter;
class ConflictSet;
/**
 * A stored procedure (transaction) in the system
 */
class Procedure : public CatalogType {
  friend class Catalog;
  friend class CatalogMap<Procedure>;

 public:
  ~Procedure();

  /** GETTER: Unique identifier for this Procedure. Allows for faster look-ups */
  int32_t GetId() const;

  /** GETTER: The full class name for the Java class for this procedure */
  const std::string & GetClassName() const;

  /** GETTER: Users authorized to invoke this procedure */
  const CatalogMap<UserRef> & GetAuthUsers() const;

  /** GETTER: Groups authorized to invoke this procedure */
  const CatalogMap<GroupRef> & GetAuthGroups() const;

  /** GETTER: Can the stored procedure modify data */
  bool IsReadOnly() const;

  /** GETTER: Does the stored procedure need data on more than one partition? */
  bool IsSinglePartition() const;

  /** GETTER: Does the stored procedure as a single procedure txn at every site? */
  bool IsEverySite() const;

  /** GETTER: Is this procedure an internal system procedure? */
  bool IsSystemProc() const;

  /** GETTER: Is this procedure a Map/Reduce procedure? */
  bool IsMapreduce() const;

  /** GETTER: Does this Procedure have Statements can be pre-fetched for distributed transactions? */
  bool IsPrefetchable() const;

  /** GETTER: Does this Procedure have at least one deferrable Statement? */
  bool IsDeferrable() const;

  /** GETTER: The name of the query that gets executed and fed into the Map function */
  const std::string & GetMapInputQuery() const;

  /** GETTER: The name of the table that the Map function will store data in */
  const std::string & GetMapEmitTable() const;

  /** GETTER: The name of the query that gets executed and fed into the Reduce function */
  const std::string & GetReduceInputQuery() const;

  /** GETTER: The name of the table that the Reduce function will store data in */
  const std::string & GetReduceEmitTable() const;

  /** GETTER: Is this a full java stored procedure or is it just a single stmt? */
  bool IsStoredProcedure() const;

  /** GETTER: Which table contains the partition column for this procedure? */
  const Table *GetPartitionTable() const;

  /** GETTER: Which column in the partitioned table is this procedure mapped on? */
  const Column *GetPartitionColumn() const;

  /** GETTER: Which parameter identifies the partition column? */
  int32_t GetPartitionParameter() const;

  /** GETTER: The set of authorized programs for this procedure (users) */
  const CatalogMap<AuthProgram> & GetAuthPrograms() const;

  /** GETTER: The set of SQL statements this procedure may call */
  const CatalogMap<Statement> & GetStatements() const;

  /** GETTER: The set of parameters to this stored procedure */
  const CatalogMap<ProcParameter> & GetParameters() const;

  /** GETTER: The conflict sets that this stored procedure has with other procedures */
  const CatalogMap<ConflictSet> & GetConflicts() const;


 protected:
  Procedure(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

  CatalogMap<UserRef> m_auth_users;

  CatalogMap<GroupRef> m_auth_groups;

  CatalogMap<AuthProgram> m_auth_programs;

  CatalogMap<Statement> m_statements;

  CatalogMap<ProcParameter> m_parameters;

  CatalogMap<ConflictSet> m_conflicts;

  int32_t m_id;

  std::string m_classname;

  bool m_read_only;

  bool m_single_partition;

  bool m_every_site;

  bool m_system_proc;

  bool m_mapreduce;

  bool m_prefetchable;

  bool m_deferrable;

  std::string m_map_input_query;

  std::string m_map_emit_table;

  std::string m_reduce_input_query;

  std::string m_reduce_emit_table;

  bool m_has_java;

  CatalogType *m_partition_table;

  CatalogType *m_partition_column;

  int32_t m_partition_parameter;

  virtual void Update();

  virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
  virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
  virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);
};

} // End catalog namespace
} // End nstore namespace
