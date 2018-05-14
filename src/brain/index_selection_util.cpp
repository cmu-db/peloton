//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection_util.cpp
//
// Identification: src/brain/index_selection_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection_util.h"
#include "common/logger.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// IndexObject
//===--------------------------------------------------------------------===//

const std::string HypotheticalIndexObject::ToString() const {
  std::stringstream str_stream;
  str_stream << "Database: " << db_oid << "\n";
  str_stream << "Table: " << table_oid << "\n";
  str_stream << "Columns: ";
  for (auto col : column_oids) {
    str_stream << col << ", ";
  }
  str_stream << "\n";
  return str_stream.str();
}

bool HypotheticalIndexObject::operator==(
    const HypotheticalIndexObject &obj) const {
  return (db_oid == obj.db_oid && table_oid == obj.table_oid &&
          column_oids == obj.column_oids);
}

bool HypotheticalIndexObject::IsCompatible(
    std::shared_ptr<HypotheticalIndexObject> index) const {
  return (db_oid == index->db_oid) && (table_oid == index->table_oid);
}

HypotheticalIndexObject HypotheticalIndexObject::Merge(
    std::shared_ptr<HypotheticalIndexObject> index) {
  HypotheticalIndexObject result;
  result.db_oid = db_oid;
  result.table_oid = table_oid;
  result.column_oids = column_oids;
  for (auto column : index->column_oids) {
    if (std::find(column_oids.begin(), column_oids.end(), column) ==
        column_oids.end())
      result.column_oids.push_back(column);
  }
  return result;
}

//===--------------------------------------------------------------------===//
// IndexConfiguration
//===--------------------------------------------------------------------===//

void IndexConfiguration::Merge(IndexConfiguration &config) {
  auto indexes = config.GetIndexes();
  for (auto it = indexes.begin(); it != indexes.end(); it++) {
    indexes_.insert(*it);
  }
}

void IndexConfiguration::Set(IndexConfiguration &config) {
  indexes_.clear();
  auto indexes = config.GetIndexes();
  for (auto it = indexes.begin(); it != indexes.end(); it++) {
    indexes_.insert(*it);
  }
}

void IndexConfiguration::RemoveIndexObject(
    const std::shared_ptr<HypotheticalIndexObject> &index_info) {
  indexes_.erase(index_info);
}

void IndexConfiguration::AddIndexObject(
    const std::shared_ptr<HypotheticalIndexObject> &index_info) {
  indexes_.insert(index_info);
}

size_t IndexConfiguration::GetIndexCount() const { return indexes_.size(); }

bool IndexConfiguration::IsEmpty() const { return indexes_.empty(); }

const std::set<std::shared_ptr<HypotheticalIndexObject>>
    &IndexConfiguration::GetIndexes() const {
  return indexes_;
}

const std::string IndexConfiguration::ToString() const {
  std::stringstream str_stream;
  str_stream << "Num of indexes: " << GetIndexCount() << "\n";
  for (auto index : indexes_) {
    str_stream << index->ToString() << " ";
  }
  return str_stream.str();
}

bool IndexConfiguration::operator==(const IndexConfiguration &config) const {
  auto config_indexes = config.GetIndexes();
  return indexes_ == config_indexes;
}

IndexConfiguration IndexConfiguration::operator-(
    const IndexConfiguration &config) {
  auto config_indexes = config.GetIndexes();

  std::set<std::shared_ptr<HypotheticalIndexObject>> result;
  std::set_difference(indexes_.begin(), indexes_.end(), config_indexes.begin(),
                      config_indexes.end(),
                      std::inserter(result, result.end()));
  return IndexConfiguration(result);
}

void IndexConfiguration::Clear() { indexes_.clear(); }

//===--------------------------------------------------------------------===//
// IndexObjectPool
//===--------------------------------------------------------------------===//

std::shared_ptr<HypotheticalIndexObject> IndexObjectPool::GetIndexObject(
    HypotheticalIndexObject &obj) {
  auto ret = map_.find(obj);
  if (ret != map_.end()) {
    return ret->second;
  }
  return nullptr;
}

std::shared_ptr<HypotheticalIndexObject> IndexObjectPool::PutIndexObject(
    HypotheticalIndexObject &obj) {
  auto index_s_ptr = GetIndexObject(obj);
  if (index_s_ptr != nullptr) return index_s_ptr;
  HypotheticalIndexObject *index_copy = new HypotheticalIndexObject();
  *index_copy = obj;
  index_s_ptr = std::shared_ptr<HypotheticalIndexObject>(index_copy);
  map_[*index_copy] = index_s_ptr;
  return index_s_ptr;
}

//===--------------------------------------------------------------------===//
// Workload
//===--------------------------------------------------------------------===//

Workload::Workload(std::vector<std::string> &queries, std::string database_name,
                   concurrency::TransactionContext *txn)
    : database_name(database_name) {
  LOG_TRACE("Initializing workload with %ld queries", queries.size());
  std::unique_ptr<binder::BindNodeVisitor> binder(
      new binder::BindNodeVisitor(txn, database_name));

  // Parse and bind every query. Store the results in the workload vector.
  for (auto query : queries) {
    LOG_DEBUG("Query: %s", query.c_str());

    // TODO: Remove this.
    // Hack to filter out pg_catalog queries.
    if (query.find("pg_") != std::string::npos) {
      continue;
    }

    // Create a unique_ptr to free this pointer at the end of this loop
    // iteration.
    auto stmt_list = std::unique_ptr<parser::SQLStatementList>(
        parser::PostgresParser::ParseSQLString(query));
    PELOTON_ASSERT(stmt_list->is_valid);
    // TODO[vamshi]: Only one query for now.
    PELOTON_ASSERT(stmt_list->GetNumStatements() == 1);

    // Create a new shared ptr from the unique ptr because
    // these queries will be referenced by multiple objects later.
    // Release the unique ptr from the stmt list to avoid freeing at the end
    // of this loop iteration.
    auto stmt = stmt_list->PassOutStatement(0);
    auto stmt_shared = std::shared_ptr<parser::SQLStatement>(stmt.release());
    PELOTON_ASSERT(stmt_shared->GetType() != StatementType::INVALID);

    try {
      // Bind the query
      binder->BindNameToNode(stmt_shared.get());
    } catch (Exception e) {
      LOG_DEBUG("Cannot bind this query");
      continue;
    }

    // Only take the DML queries from the workload
    switch (stmt_shared->GetType()) {
      case StatementType::INSERT:
      case StatementType::DELETE:
      case StatementType::UPDATE:
      case StatementType::SELECT:
        AddQuery(stmt_shared);
      default:
        // Ignore other queries.
        LOG_TRACE("Ignoring query: %s", stmt->GetInfo().c_str());
    }
  }
}

}  // namespace brain
}  // namespace peloton
