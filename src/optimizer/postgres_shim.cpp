//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_shim.cpp
//
// Identification: src/optimizer/postgres_shim.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "c.h"
#include "postgres.h"
#include "nodes/memnodes.h"

#include "optimizer/postgres_shim.h"
#include "optimizer/query_transformer.h"
#include "optimizer/query_operators.h"
#include "optimizer/query_node_printer.h"

#include "access/attnum.h"
#include "miscadmin.h"
#include "storage/lock.h"
#include "access/heapam.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "catalog/pg_namespace.h"

namespace peloton {
namespace optimizer {

namespace {

} // anonymous namespace

//===--------------------------------------------------------------------===//
// Compatibility with Postgres
//===--------------------------------------------------------------------===//
bool ShouldPelotonOptimize(Query *parse) {
  bool should_optimize = false;

  if (!IsPostmasterEnvironment && !IsBackend) {
    return false;
  }

  if (parse != NULL && parse->rtable != NULL) {
    ListCell *l;

    // Go over each relation on which the plan depends
    foreach(l, parse->rtable) {
      RangeTblEntry *rte = static_cast<RangeTblEntry *>(lfirst(l));

      if (rte->rtekind == RTE_RELATION) {
        Oid relation_id = rte->relid;
        // Check if relation in public namespace
        Relation target_table = relation_open(relation_id, AccessShareLock);
        Oid target_table_namespace = target_table->rd_rel->relnamespace;

        should_optimize = (target_table_namespace == PG_PUBLIC_NAMESPACE);

        relation_close(target_table, AccessShareLock);

        if (should_optimize)
          break;
      }
    }
  }
  return should_optimize;
}

std::shared_ptr<Select> PostgresQueryToPelotonQuery(Query *parse) {
  QueryTransformer transformer{};
  std::shared_ptr<Select> select_query{transformer.Transform(parse)};
  return select_query;
}

std::shared_ptr<planner::AbstractPlan> PelotonOptimize(
  Optimizer &optimizer,
  Query *parse,
  int cursorOptions,
  ParamListInfo boundParams)
{
  (void)boundParams;
  (void)cursorOptions;

  char* query_output = nodeToString(parse);
  LOG_DEBUG("cursorOptions %d", cursorOptions);
  LOG_DEBUG("Query: %s", query_output);
  (void)query_output;

  std::shared_ptr<Select> query_tree = PostgresQueryToPelotonQuery(parse);
  std::shared_ptr<planner::AbstractPlan> plan;
  if (query_tree) {
    LOG_DEBUG("Succesfully converted postgres query to Peloton query");
    LOG_DEBUG("Peloton query:\n %s", PrintQuery(query_tree.get()).c_str());
    LOG_DEBUG("Invoking peloton optimizer...");
    plan = optimizer.GeneratePlan(query_tree);
    if (plan) {
      LOG_DEBUG("Succesfully converted Peloton query into Peloton plan");
    }
  }

  return plan;
}

}
}
