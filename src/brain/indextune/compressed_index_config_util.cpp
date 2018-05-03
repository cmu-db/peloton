//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compressed_index_config.cpp
//
// Identification: src/brain/indextune/compressed_index_config.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/indextune/compressed_index_config_util.h"

namespace peloton {
namespace brain {

void
CompressedIndexConfigUtil::AddCandidates(
    CompressedIndexConfigContainer &container, const std::string &query,
    boost::dynamic_bitset<>& add_candidates) {
  add_candidates = boost::dynamic_bitset<>(container.GetConfigurationCount());
  auto sql_stmt_list = ToBindedSqlStmtList(container, query);
  auto txn = container.GetTransactionManager()->BeginTransaction();
  container.GetCatalog()->GetDatabaseObject(container.GetDatabaseName(), txn);
  std::vector<planner::col_triplet> affected_cols_vector =
      planner::PlanUtil::GetIndexableColumns(
          txn->catalog_cache,
          std::move(sql_stmt_list),
          container.GetDatabaseName());
  container.GetTransactionManager()->CommitTransaction(txn);

  // Aggregate all columns in the same table
  std::unordered_map<oid_t, brain::IndexObject> aggregate_map;

  for (const auto &each_triplet : affected_cols_vector) {
    const auto db_oid = std::get<0>(each_triplet);
    const auto table_oid = std::get<1>(each_triplet);
    const auto col_oid = std::get<2>(each_triplet);

    if (aggregate_map.find(table_oid) == aggregate_map.end()) {
      aggregate_map[table_oid] = brain::IndexObject();
      aggregate_map.at(table_oid).db_oid = db_oid;
      aggregate_map.at(table_oid).table_oid = table_oid;
    }

    aggregate_map.at(table_oid).column_oids.insert(col_oid);
  }

  const auto db_oid = aggregate_map.begin()->second.db_oid;

  for (const auto it : aggregate_map) {
    const auto table_oid = it.first;
    const auto &column_oids = it.second.column_oids;
    const auto table_offset = container.GetTableOffset(table_oid);

    // Insert empty index
    add_candidates.set(table_offset);

    // For each index, iterate through its columns
    // and incrementally add the columns to the prefix closure of current table
    std::vector<oid_t> col_oids;
    for (const auto column_oid : column_oids) {
      col_oids.push_back(column_oid);

      // Insert prefix index
      auto idx_new =
          std::make_shared<brain::IndexObject>(db_oid, table_oid, col_oids);
      SetBit(container, add_candidates, idx_new);
    }
  }
}

void
CompressedIndexConfigUtil::DropCandidates(
    CompressedIndexConfigContainer &container,
    const std::string &query,
    boost::dynamic_bitset<>& drop_candidates) {
  drop_candidates = boost::dynamic_bitset<>(container.GetConfigurationCount());

  auto sql_stmt_list = ToBindedSqlStmtList(container, query);
  auto sql_stmt = sql_stmt_list->GetStatement(0);

  auto txn = container.GetTransactionManager()->BeginTransaction();
  container.GetCatalog()->GetDatabaseObject(container.GetDatabaseName(), txn);
  std::vector<planner::col_triplet> affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt,
                                            true);
  for (const auto &col_triplet : affected_indexes) {
    auto idx_obj = ConvertIndexTriplet(container, col_triplet);
    SetBit(container, drop_candidates, idx_obj);
  }
  container.GetTransactionManager()->CommitTransaction(txn);
}

std::shared_ptr<brain::IndexObject>
CompressedIndexConfigUtil::ConvertIndexTriplet(
    CompressedIndexConfigContainer &container,
    const planner::col_triplet &idx_triplet) {
  const auto db_oid = std::get<0>(idx_triplet);
  const auto table_oid = std::get<1>(idx_triplet);
  const auto idx_oid = std::get<2>(idx_triplet);

  auto txn = container.GetTransactionManager()->BeginTransaction();
  const auto db_obj = container.GetCatalog()->GetDatabaseObject(db_oid, txn);
  const auto table_obj = db_obj->GetTableObject(table_oid);
  const auto idx_obj = table_obj->GetIndexObject(idx_oid);
  const auto col_oids = idx_obj->GetKeyAttrs();
  std::vector<oid_t> input_oids(col_oids);

  container.GetTransactionManager()->CommitTransaction(txn);

  return std::make_shared<brain::IndexObject>(db_oid, table_oid, input_oids);
}

std::unique_ptr<parser::SQLStatementList>
CompressedIndexConfigUtil::ToBindedSqlStmtList(
    CompressedIndexConfigContainer &container,
    const std::string &query_string) {
  auto txn = container.GetTransactionManager()->BeginTransaction();
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  auto sql_stmt = sql_stmt_list->GetStatement(0);
  auto bind_node_visitor =
      binder::BindNodeVisitor(txn, container.GetDatabaseName());
  bind_node_visitor.BindNameToNode(sql_stmt);
  container.GetTransactionManager()->CommitTransaction(txn);

  return sql_stmt_list;
}

std::unique_ptr<boost::dynamic_bitset<>>
CompressedIndexConfigUtil::GenerateBitSet(
    const CompressedIndexConfigContainer &container,
    const std::vector<std::shared_ptr<brain::IndexObject>> &idx_objs) {
  auto result = std::unique_ptr<boost::dynamic_bitset<>>(
      new boost::dynamic_bitset<>(container.GetConfigurationCount()));

  for (const auto &idx_obj : idx_objs) {
    SetBit(container, *result, idx_obj);
  }

  return result;
}

void CompressedIndexConfigUtil::SetBit(
    const CompressedIndexConfigContainer &container,
    boost::dynamic_bitset<> &bitmap,
    const std::shared_ptr<IndexObject> &idx_object) {
  size_t offset = container.GetGlobalOffset(idx_object);
  bitmap.set(offset);
}

void CompressedIndexConfigUtil::ConstructQueryConfigFeature(
    const boost::dynamic_bitset<> &curr_config_set,
    const boost::dynamic_bitset<> &add_candidate_set,
    const boost::dynamic_bitset<> &drop_candidate_set,
    vector_eig &query_config_vec) {
  size_t num_configs = curr_config_set.size();
  query_config_vec = vector_eig::Zero(2 * num_configs);
  size_t offset_rec = 0;
  size_t config_id_rec = add_candidate_set.find_first();
  query_config_vec[offset_rec] = 1.0;
  while (config_id_rec != boost::dynamic_bitset<>::npos) {
    if (curr_config_set.test(config_id_rec)) {
      query_config_vec[offset_rec + config_id_rec] = 1.0f;
    } else {
      query_config_vec[offset_rec + config_id_rec] = -1.0f;
    }
    config_id_rec = add_candidate_set.find_next(config_id_rec);
  }
  size_t offset_drop = num_configs;
  size_t config_id_drop = drop_candidate_set.find_first();
  query_config_vec[offset_drop] = 1.0;
  while (config_id_drop != boost::dynamic_bitset<>::npos) {
    if (curr_config_set.test(config_id_drop)) {
      query_config_vec[offset_drop + config_id_drop] = 1.0f;
    }
    // else case shouldnt happen
    config_id_drop = drop_candidate_set.find_next(config_id_drop);
  }
}

void CompressedIndexConfigUtil::ConstructConfigFeature(
    const CompressedIndexConfigContainer &container, vector_eig &config_vec) {
  container.ToCoveredEigen(config_vec);
}
}
}