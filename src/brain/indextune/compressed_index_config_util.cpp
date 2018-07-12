//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compressed_index_config_util.cpp
//
// Identification: src/brain/indextune/compressed_index_config_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/indextune/compressed_index_config_util.h"
#include "brain/index_selection.h"

namespace peloton {
namespace brain {

void CompressedIndexConfigUtil::AddCandidates(
    CompressedIndexConfigContainer &container, const std::string &query,
    boost::dynamic_bitset<> &add_candidates, CandidateSelectionType cand_sel_type,
    size_t max_index_size, IndexSelectionKnobs knobs) {
  add_candidates = boost::dynamic_bitset<>(container.GetConfigurationCount());
  // First add all {} empty index bits
  for (const auto it : container.table_offset_map_) {
    const auto table_offset = it.second;
    add_candidates.set(table_offset);
  }
  if(cand_sel_type == CandidateSelectionType::AutoAdmin) {
    // Generate autoadmin candidates
    IndexConfiguration best_config;
    auto txn = container.GetTransactionManager()->BeginTransaction();
    std::vector<std::string> queries = {query};
    brain::Workload w = {queries, container.GetDatabaseName(), txn};
    brain::IndexSelection is = {w, knobs, txn};
    is.GetBestIndexes(best_config);
    container.GetTransactionManager()->CommitTransaction(txn);
    for(const auto& hypot_index_obj: best_config.GetIndexes()) {
      MarkPrefixClosure(container, add_candidates, hypot_index_obj);
    }
  } else if (cand_sel_type == CandidateSelectionType::Simple || cand_sel_type == CandidateSelectionType::Exhaustive) {
    auto sql_stmt_list = ToBindedSqlStmtList(container, query);
    auto txn = container.GetTransactionManager()->BeginTransaction();
    container.GetCatalog()->GetDatabaseCatalogEntry(txn, container.GetDatabaseName());

    std::vector<planner::col_triplet> indexable_cols_vector =
        planner::PlanUtil::GetIndexableColumns(txn->catalog_cache,
                                               std::move(sql_stmt_list),
                                               container.GetDatabaseName());
    container.GetTransactionManager()->CommitTransaction(txn);

    if (cand_sel_type == CandidateSelectionType::Simple) {
      for (const auto &each_triplet : indexable_cols_vector) {
        const auto db_oid = std::get<0>(each_triplet);
        const auto table_oid = std::get<1>(each_triplet);
        const auto col_oid = std::get<2>(each_triplet);

        std::vector<oid_t> col_oids = {col_oid};
        auto idx_new = std::make_shared<brain::HypotheticalIndexObject>(
            db_oid, table_oid, col_oids);

        SetBit(container, add_candidates, idx_new);
      }
    } else if (cand_sel_type == CandidateSelectionType::Exhaustive) {
      // Aggregate all columns in the same table
      std::unordered_map<oid_t, brain::HypotheticalIndexObject> aggregate_map;
      for (const auto &each_triplet : indexable_cols_vector) {
        const auto db_oid = std::get<0>(each_triplet);
        const auto table_oid = std::get<1>(each_triplet);
        const auto col_oid = std::get<2>(each_triplet);

        if (aggregate_map.find(table_oid) == aggregate_map.end()) {
          aggregate_map[table_oid] = brain::HypotheticalIndexObject();
          aggregate_map.at(table_oid).db_oid = db_oid;
          aggregate_map.at(table_oid).table_oid = table_oid;
        }

        aggregate_map.at(table_oid).column_oids.push_back(col_oid);
      }

      const auto db_oid = container.GetDatabaseOID();

      for (const auto it : aggregate_map) {
        const auto table_oid = it.first;
        const auto &column_oids = it.second.column_oids;

        // Insert empty index
        add_candidates.set(container.GetTableOffsetStart(table_oid));

        std::vector<oid_t> index_conf;

        // Insert index consisting of up to max_index_size columns
        PermuateConfigurations(container, column_oids, max_index_size, index_conf,
                               add_candidates, db_oid, table_oid);
      }
    }
  }
}

void CompressedIndexConfigUtil::DropCandidates(
    CompressedIndexConfigContainer &container, const std::string &query,
    boost::dynamic_bitset<> &drop_candidates) {
  drop_candidates = boost::dynamic_bitset<>(container.GetConfigurationCount());

  auto sql_stmt_list = ToBindedSqlStmtList(container, query);
  auto sql_stmt = sql_stmt_list->GetStatement(0);

  auto txn = container.GetTransactionManager()->BeginTransaction();
  container.GetCatalog()->GetDatabaseCatalogEntry(txn, container.GetDatabaseName());
  std::vector<planner::col_triplet> affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt,
                                            true);
  for (const auto &col_triplet : affected_indexes) {
    auto idx_obj = ConvertIndexTriplet(container, col_triplet);
    SetBit(container, drop_candidates, idx_obj);
  }
  container.GetTransactionManager()->CommitTransaction(txn);
}

std::shared_ptr<brain::HypotheticalIndexObject>
CompressedIndexConfigUtil::ConvertIndexTriplet(
    CompressedIndexConfigContainer &container,
    const planner::col_triplet &idx_triplet) {
  const auto db_oid = std::get<0>(idx_triplet);
  const auto table_oid = std::get<1>(idx_triplet);
  const auto idx_oid = std::get<2>(idx_triplet);

  auto txn = container.GetTransactionManager()->BeginTransaction();
  const auto db_obj = container.GetCatalog()->GetDatabaseCatalogEntry(txn, db_oid);
  const auto table_obj = db_obj->GetTableCatalogEntry(table_oid);
  const auto idx_obj = table_obj->GetIndexCatalogEntries(idx_oid);
  const auto col_oids = idx_obj->GetKeyAttrs();
  std::vector<oid_t> input_oids(col_oids);

  container.GetTransactionManager()->CommitTransaction(txn);

  return std::make_shared<brain::HypotheticalIndexObject>(db_oid, table_oid,
                                                          input_oids);
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
    const std::vector<std::shared_ptr<brain::HypotheticalIndexObject>>
        &idx_objs) {
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
    const std::shared_ptr<HypotheticalIndexObject> &idx_object) {
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

  // Featurization mechanism: Add candidates
  // 1 if idx belongs to add cand set + current state config
  // -1 if idx belongs to add cand set + not in curr state config
  // 0 otherwise
  size_t offset_rec = 0;
  // TODO(saatviks): Disabling this for now
//   query_config_vec[offset_rec] = 1.0;
  size_t config_id_rec = add_candidate_set.find_first();
  while (config_id_rec != boost::dynamic_bitset<>::npos) {
    if (curr_config_set.test(config_id_rec)) {
      query_config_vec[offset_rec + config_id_rec] = 1.0f;
    } else {
      query_config_vec[offset_rec + config_id_rec] = -1.0f;
    }
    config_id_rec = add_candidate_set.find_next(config_id_rec);
  }

  // Featurization mechanism: Drop candidates
  // 1 if idx belongs to drop cand set + current state config
  // 0 otherwise
  size_t offset_drop = num_configs;
  size_t config_id_drop = drop_candidate_set.find_first();
  // TODO(saatviks): Disabling this for now
//   query_config_vec[offset_drop] = 1.0;
  while (config_id_drop != boost::dynamic_bitset<>::npos) {
    if (curr_config_set.test(config_id_drop)) {
      query_config_vec[offset_drop + config_id_drop] = 1.0f;
    }
    config_id_drop = drop_candidate_set.find_next(config_id_drop);
  }
}

void CompressedIndexConfigUtil::GetIgnoreTables(
    const std::string &db_name, std::set<oid_t> &ori_table_oids) {
  peloton::concurrency::TransactionManager *txn_manager =
      &concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager->BeginTransaction();
  const auto table_objs = catalog::Catalog::GetInstance()
                              ->GetDatabaseCatalogEntry(txn, db_name)
                              ->GetTableCatalogEntries();

  for (const auto &it : table_objs) {
    ori_table_oids.insert(it.first);
  }

  txn_manager->CommitTransaction(txn);
}

void CompressedIndexConfigUtil::ConstructStateConfigFeature(
    const boost::dynamic_bitset<> &config_set, vector_eig &config_vec) {
  // Note that the representation is reversed - but this should not affect
  // anything
  config_vec = -vector_eig::Ones(config_set.size());
  size_t config_id = config_set.find_first();
  while (config_id != boost::dynamic_bitset<>::npos) {
    config_vec[config_id] = 1.0;
    config_id = config_set.find_next(config_id);
  }
}

IndexConfiguration CompressedIndexConfigUtil::ToIndexConfiguration(
    const CompressedIndexConfigContainer &container) {
  brain::IndexConfiguration index_config;

  for (const auto it : container.table_offset_map_) {
    const auto start_idx = it.second;
    size_t end_idx = container.GetNextTableIdx(start_idx);
    auto idx = start_idx;
    while (idx != boost::dynamic_bitset<>::npos && idx < end_idx) {
      auto hypo_index_obj = container.GetIndex(idx);
      index_config.AddIndexObject(hypo_index_obj);
      idx = container.GetNextSetIndexConfig(idx);
    }
  }

  return index_config;
}

void CompressedIndexConfigUtil::PermuateConfigurations(
    const CompressedIndexConfigContainer &container,
    const std::vector<oid_t> &cols, size_t max_index_size,
    std::vector<oid_t> &index_conf, boost::dynamic_bitset<> &bitset,
    oid_t db_oid, oid_t table_oid) {
  if (index_conf.size() <= std::min<size_t>(max_index_size, cols.size())) {
    auto idx_new = std::make_shared<brain::HypotheticalIndexObject>(
        db_oid, table_oid, index_conf);
    SetBit(container, bitset, idx_new);
  }
  for (auto col : cols) {
    if (std::find(index_conf.begin(), index_conf.end(), col) ==
        index_conf.end()) {
      index_conf.push_back(col);
      PermuateConfigurations(container, cols, max_index_size, index_conf,
                             bitset, db_oid, table_oid);
      index_conf.pop_back();
    }
  }
}

void CompressedIndexConfigUtil::MarkPrefixClosure(const CompressedIndexConfigContainer &container,
                                                  boost::dynamic_bitset<> &bitset,
                                                  const std::shared_ptr<HypotheticalIndexObject> &hypot_index_obj) {
  auto &col_oids = hypot_index_obj->column_oids;
  for(size_t i = 1; i <= hypot_index_obj->column_oids.size(); i++) {
    auto index_conf = std::vector<oid_t>(col_oids.begin(), col_oids.begin() + i);
    auto idx_new = std::make_shared<brain::HypotheticalIndexObject>(
        hypot_index_obj->db_oid, hypot_index_obj->table_oid, index_conf);
    SetBit(container, bitset, idx_new);
  }
}

std::string CompressedIndexConfigUtil::ToString(
    std::vector<oid_t> config_vector) {
  std::stringstream str_stream;
  str_stream << "(";
  for (auto idx : config_vector) {
    str_stream << idx << ",";
  }
  str_stream << ")" << std::endl;
  return str_stream.str();
}

std::string CompressedIndexConfigUtil::ToString(peloton::vector_eig v) {
  std::stringstream str_stream;
  str_stream << v.transpose() << std::endl;
  return str_stream.str();
}

}  // namespace brain
}  // namespace peloton