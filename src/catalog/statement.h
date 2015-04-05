#pragma once

#include <string>
#include "catalog_map.h"
#include "catalog_type.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Statement
//===--------------------------------------------------------------------===//

class StmtParameter;
class Column;
class PlanFragment;
/**
 * A parameterized SQL statement embedded in a stored procedure
 */
class Statement : public CatalogType {
  friend class Catalog;
  friend class CatalogMap<Statement>;

 public:
  ~Statement();

  /** GETTER: Unique identifier for this Procedure. Allows for faster look-ups */
  int32_t GetId() const;

  /** GETTER: The text of the sql statement */
  const std::string & GetSqlText() const;

  int32_t GetQueryType() const;

  /** GETTER: Can the statement modify any data? */
  bool IsReadOnly() const;

  /** GETTER: Does the statement only use data on one partition? */
  bool IsSinglePartition() const;

  /** GETTER: Should the result of this statememt be divided by partition count before returned */
  bool IsReplicatedTableDML() const;

  /** GETTER: Does this statement only access replicated tables? */
  bool IsReplicatedOnly() const;

  bool IsBatched() const;

  bool UsesSecondaryIndex() const;

  /** GETTER: Whether this query should be examined for pre-fetching if Procedure is being executed as a distributed transaction */
  bool IsPrefetchable() const;

  /** GETTER: Whether this query does not need to executed immediately in this transaction */
  bool IsDeferrable() const;

  int32_t GetParamNum() const;

  /** GETTER: The set of parameters to this SQL statement */
  const CatalogMap<StmtParameter> & GetParameters() const;

  /** GETTER: The set of columns in the output table */
  const CatalogMap<Column> & GetOutputColumns() const;

  /** GETTER: Whether this statement has a single-sited query plan */
  bool HasSingleSitedPlan() const;

  /** GETTER: The set of plan fragments used to execute this statement */
  const CatalogMap<PlanFragment> & GetFragments() const;

  /** GETTER: A serialized representation of the original expression tree */
  const std::string & GetExpressionTree() const;

  /** GETTER: A serialized representation of the un-fragmented plan */
  const std::string & GetFullPlan() const;

  /** GETTER: Whether this statement has a multi-sited query plan */
  bool HasMultiSitedPlan() const;

  /** GETTER: The set of multi-sited plan fragments used to execute this statement */
  const CatalogMap<PlanFragment> & GetMSFragments() const;

  /** GETTER: A serialized representation of the multi-sited query plan */
  const std::string & GetMSExpressionTree() const;

  /** GETTER: A serialized representation of the multi-sited query plan */
  const std::string & GetMSFullPlan() const;

  /** GETTER: The cost of this plan measured in arbitrary units */
  int32_t cost() const;

 protected:
  Statement(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);

  CatalogMap<StmtParameter> m_parameters;

  CatalogMap<Column> m_output_columns;

  CatalogMap<PlanFragment> m_fragments;

  CatalogMap<PlanFragment> m_ms_fragments;

  int32_t m_id;

  std::string m_sql_text;

  int32_t m_query_type;

  bool m_read_only;

  bool m_single_partition;

  bool m_replicated_table_dml;

  bool m_replicated_only;

  bool m_batched;

  bool m_secondary_index;

  bool m_prefetchable;

  bool m_deferrable;

  int32_t m_param_num;

  bool m_has_single_sited;

  std::string m_exp_tree;

  std::string m_full_plan;

  bool m_has_multi_sited;

  std::string m_ms_exptree;

  std::string m_ms_fullplan;

  int32_t m_cost;

  virtual void Update();

  virtual CatalogType * AddChild(const std::string &collection_name, const std::string &name);
  virtual CatalogType * GetChild(const std::string &collection_name, const std::string &child_name) const;
  virtual bool RemoveChild(const std::string &collection_name, const std::string &child_name);


};

} // End catalog namespace
} // End nstore namespace
