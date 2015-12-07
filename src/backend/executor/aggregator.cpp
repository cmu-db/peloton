//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// aggregator.cpp
//
// Identification: src/backend/executor/aggregator.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <set>

#include "backend/executor/aggregator.h"
#include "backend/executor/executor_context.h"
#include "backend/common/logger.h"
#include "backend/storage/data_table.h"

namespace peloton {
namespace executor {

/*
 * Create an instance of an aggregator for the specified aggregate
 * type, column type, and result type. The object is constructed in
 * memory from the provided memrory pool.
 */
Agg *GetAggInstance(ExpressionType agg_type) {
  Agg *aggregator;

  switch (agg_type) {
    case EXPRESSION_TYPE_AGGREGATE_COUNT:
      aggregator = new CountAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR:
      aggregator = new CountStarAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_SUM:
      aggregator = new SumAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_AVG:
      aggregator = new AvgAgg(false);
      break;
    case EXPRESSION_TYPE_AGGREGATE_MIN:
      aggregator = new MinAgg();
      break;
    case EXPRESSION_TYPE_AGGREGATE_MAX:
      aggregator = new MaxAgg();
      break;
    default: {
      std::string message = "Unknown aggregate type "
          + std::to_string(agg_type);
      throw UnknownTypeException(agg_type, message);
    }
  }

  return aggregator;
}

/* Handle distinct */
Agg::~Agg() {
  if (is_distinct_) {
    for (auto& val : distinct_set_) {
      // TODO: Not sure if we need to do this ?
      val.Free();
    }
  }
}

void Agg::Advance(const Value val) {
  if (is_distinct_) {
    // Insert a deep copy
    distinct_set_.insert(ValueFactory::Clone(val, nullptr));
  } else {
    DAdvance(val);
  }

}

Value Agg::Finalize() {
  if (is_distinct_) {
    for (auto val : distinct_set_) {
      DAdvance(val);
    }
  }
  return DFinalize();
}

/*
 * Helper method responsible for inserting the results of the aggregation
 * into a new tuple in the output tile group as well as passing through any
 * additional columns from the input tile group.
 *
 * Output tuple is projected from two tuples:
 * Left is the 'delegate' tuple, which is usually the first tuple in the group,
 * used to retrieve pass-through values;
 * Right is the tuple holding all aggregated values.
 */
bool Helper(const planner::AggregatePlan *node, Agg **aggregates,
            storage::DataTable *output_table,
            const AbstractTuple *delegate_tuple,
            executor::ExecutorContext* econtext) {

  auto schema = output_table->GetSchema();
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));

  /*
   * 1) Construct a vector of aggregated values
   */
  std::vector<Value> aggregate_values;
  auto& aggregate_terms = node->GetUniqueAggTerms();
  for (oid_t column_itr = 0; column_itr < aggregate_terms.size();
      column_itr++) {
    if (aggregates[column_itr] != nullptr) {
      Value final_val = aggregates[column_itr]->Finalize();
      aggregate_values.push_back(final_val);
    }
  }

  /*
   * 2) Evaluate filter predicate;
   * if fail, just return
   */
  std::unique_ptr<expression::ContainerTuple<std::vector<Value>> > aggref_tuple(
      new expression::ContainerTuple<std::vector<Value>>(&aggregate_values));

  auto predicate = node->GetPredicate();
  if (nullptr != predicate
      && predicate->Evaluate(delegate_tuple, aggref_tuple.get(), econtext)
          .IsFalse()) {
    return true;  // Qual fails, do nothing
  }

  /*
   * 3) Construct the tuple to insert using projectInfo
   */
  node->GetProjectInfo()->Evaluate(tuple.get(), delegate_tuple,
                                   aggref_tuple.get(), econtext);

  LOG_TRACE("Tuple to Output :");
  LOG_TRACE("GROUP TUPLE :: %s", tuple->GetInfo().c_str());

  auto location = output_table->InsertTuple(econtext->GetTransaction(),
                                            tuple.get());
  if (location.block == INVALID_OID) {
    LOG_ERROR("Failed to insert tuple \n");
    return false;
  }

  return true;
}

//===--------------------------------------------------------------------===//
// Hash Aggregator
//===--------------------------------------------------------------------===//
HashAggregator::HashAggregator(const planner::AggregatePlan *node,
                               storage::DataTable *output_table,
                               executor::ExecutorContext* econtext,
                               size_t num_input_columns)
    : AbstractAggregator(node, output_table, econtext),
      num_input_columns(num_input_columns) {

  group_by_key_values.resize(node->GetGroupbyColIds().size(),
                             ValueFactory::GetNullValue());

}

HashAggregator::~HashAggregator() {

  for (auto entry : aggregates_map) {
    // Clean up allocated storage
    for (size_t aggno = 0; aggno < node->GetUniqueAggTerms().size(); aggno++) {
      delete entry.second->aggregates[aggno];
    }
    delete[] entry.second->aggregates;

    delete entry.second;
  }

}

bool HashAggregator::Advance(AbstractTuple *cur_tuple) {

  AggregateList *aggregate_list;

  // Configure a group-by-key and search for the required group.
  for (oid_t column_itr = 0; column_itr < node->GetGroupbyColIds().size();
      column_itr++) {
    Value cur_tuple_val = cur_tuple->GetValue(
        node->GetGroupbyColIds()[column_itr]);
    group_by_key_values[column_itr] = cur_tuple_val;
  }

  auto map_itr = aggregates_map.find(group_by_key_values);

  // Group not found. Make a new entry in the hash for this new group.
  if (map_itr == aggregates_map.end()) {
    LOG_TRACE("Group-by key not found. Start a new group.");
    // Allocate new aggregate list
    aggregate_list = new AggregateList();
    aggregate_list->aggregates = new Agg *[node->GetUniqueAggTerms().size()];
    // Make a deep copy of the first tuple we meet
    for (size_t col_id = 0; col_id < num_input_columns; col_id++) {
      aggregate_list->first_tuple_values.push_back(
          ValueFactory::Clone(cur_tuple->GetValue(col_id), nullptr));
    };

    for (oid_t aggno = 0; aggno < node->GetUniqueAggTerms().size(); aggno++) {
      aggregate_list->aggregates[aggno] = GetAggInstance(
          node->GetUniqueAggTerms()[aggno].aggtype);

      bool distinct = node->GetUniqueAggTerms()[aggno].distinct;
      aggregate_list->aggregates[aggno]->SetDistinct(distinct);
    }

    aggregates_map.insert(
        HashAggregateMapType::value_type(group_by_key_values, aggregate_list));
  }
// Otherwise, the list is the second item of the pair.
  else {
    aggregate_list = map_itr->second;
  }

// Update the aggregation calculation
  for (oid_t aggno = 0; aggno < node->GetUniqueAggTerms().size(); aggno++) {
    auto predicate = node->GetUniqueAggTerms()[aggno].expression;
    Value value = ValueFactory::GetIntegerValue(1);
    if (predicate) {
      value = node->GetUniqueAggTerms()[aggno].expression->Evaluate(
          cur_tuple, nullptr, this->executor_context);
    }
    aggregate_list->aggregates[aggno]->Advance(value);
  }

  return true;
}

bool HashAggregator::Finalize() {
  for (auto entry : aggregates_map) {
    // Construct a container for the first tuple
    expression::ContainerTuple<std::vector<Value>> first_tuple(
        &entry.second->first_tuple_values);
    if (Helper(node, entry.second->aggregates, output_table, &first_tuple,
               this->executor_context) == false) {
      return false;
    }
  }

  return true;
}

//===--------------------------------------------------------------------===//
// Sort Aggregator
//===--------------------------------------------------------------------===//

SortedAggregator::SortedAggregator(const planner::AggregatePlan *node,
                                   storage::DataTable *output_table,
                                   executor::ExecutorContext* econtext,
                                   size_t num_input_columns)
    : AbstractAggregator(node, output_table, econtext),
      delegate_tuple_(&delegate_tuple_values_), // Bind value vector to wrapper container tuple
      num_input_columns_(num_input_columns) {

  aggregates = new Agg*[node->GetUniqueAggTerms().size()];
  ::memset(aggregates, 0, sizeof(Agg *) * node->GetUniqueAggTerms().size());

  assert(delegate_tuple_values_.empty());

}

SortedAggregator::~SortedAggregator() {
  // Clean up aggregators
  for (oid_t column_itr = 0; column_itr < node->GetUniqueAggTerms().size();
      column_itr++) {
    delete aggregates[column_itr];
  }
  delete[] aggregates;

}

bool SortedAggregator::Advance(AbstractTuple *next_tuple) {
  bool start_new_agg = false;

// Check if we are starting a new aggregate tuple
  if (delegate_tuple_values_.empty()) {  // No current group
    LOG_TRACE("Current group keys are empty!");
    start_new_agg = true;
  } else {  // Current group exists
    assert(delegate_tuple_values_.size() == num_input_columns_);
    // Check whether crossed group boundary
    for (oid_t grpColOffset = 0; grpColOffset < node->GetGroupbyColIds().size();
        grpColOffset++) {
      Value lval = next_tuple->GetValue(node->GetGroupbyColIds()[grpColOffset]);
      Value rval = delegate_tuple_.GetValue(node->GetGroupbyColIds()[grpColOffset]);
      bool not_equal = lval.OpNotEquals(rval).IsTrue();

      if (not_equal) {
        LOG_TRACE("Group-by columns changed.");

        // Call helper to output the current group result
        if (!Helper(node, aggregates, output_table, &delegate_tuple_,
                    this->executor_context)) {
          return false;
        }

        start_new_agg = true;
        break;
      }
    }
  }

  // If we have started a new aggregate tuple
  if (start_new_agg) {
    LOG_TRACE("Started a new group!");

    // Create aggregate
    for (oid_t aggno = 0; aggno < node->GetUniqueAggTerms().size(); aggno++) {
      // Clean up previous aggregate
      delete aggregates[aggno];
      aggregates[aggno] = GetAggInstance(
          node->GetUniqueAggTerms()[aggno].aggtype);

      bool distinct = node->GetUniqueAggTerms()[aggno].distinct;
      aggregates[aggno]->SetDistinct(distinct);
    }

    // Update delegate tuple values
    delegate_tuple_values_.clear();

    for(oid_t col_id = 0; col_id < num_input_columns_; col_id++){
      Value val = next_tuple->GetValue(col_id);
      delegate_tuple_values_.push_back(ValueFactory::Clone(val, nullptr));
    }
  }

  // Update the aggregation calculation
  for (oid_t aggno = 0; aggno < node->GetUniqueAggTerms().size(); aggno++) {
    auto predicate = node->GetUniqueAggTerms()[aggno].expression;
    Value value = ValueFactory::GetIntegerValue(1);
    if (predicate) {
      value = node->GetUniqueAggTerms()[aggno].expression->Evaluate(
          next_tuple, nullptr, this->executor_context);
    }
    aggregates[aggno]->Advance(value);
  }

  return true;
}

bool SortedAggregator::Finalize() {

  // Call helper to output the current group result
  if (!delegate_tuple_values_.empty()
      && !Helper(node, aggregates, output_table, &delegate_tuple_,
                 this->executor_context)) {
    return false;
  }

  return true;
}

//===--------------------------------------------------------------------===//
// Plain Aggregator
//===--------------------------------------------------------------------===//
PlainAggregator::PlainAggregator(const planner::AggregatePlan* node,
                                 storage::DataTable* output_table,
                                 executor::ExecutorContext* econtext)
    : AbstractAggregator(node, output_table, econtext) {

  // allocate aggregators
  aggregates = new Agg*[node->GetUniqueAggTerms().size()];
  ::memset(aggregates, 0, sizeof(Agg *) * node->GetUniqueAggTerms().size());

  // initialize aggregators
  for (oid_t aggno = 0; aggno < node->GetUniqueAggTerms().size(); aggno++) {
    aggregates[aggno] = GetAggInstance(
        node->GetUniqueAggTerms()[aggno].aggtype);

    bool distinct = node->GetUniqueAggTerms()[aggno].distinct;
    aggregates[aggno]->SetDistinct(distinct);
  }

}

bool PlainAggregator::Advance(AbstractTuple* next_tuple) {
  // Update the aggregation calculation
  for (oid_t aggno = 0; aggno < node->GetUniqueAggTerms().size(); aggno++) {
    auto predicate = node->GetUniqueAggTerms()[aggno].expression;
    Value value = ValueFactory::GetIntegerValue(1);
    if (predicate) {
      value = node->GetUniqueAggTerms()[aggno].expression->Evaluate(
          next_tuple, nullptr, this->executor_context);
    }
    aggregates[aggno]->Advance(value);
  }
  return true;
}

bool PlainAggregator::Finalize() {
  if (!Helper(node, aggregates, output_table, nullptr,
              this->executor_context)) {
    return false;
  }

  // TODO: if no record exists in input_table, we have to output a null record
  // only when it doesn't have GROUP BY. See difference of these cases:
  //   SELECT SUM(A) FROM BBB ,   when BBB has no tuple
  //   SELECT SUM(A) FROM BBB GROUP BY C,   when BBB has no tuple

  return true;

}

PlainAggregator::~PlainAggregator() {
  // Clean up aggregators
  for (oid_t column_itr = 0; column_itr < node->GetUniqueAggTerms().size();
      column_itr++) {
    delete aggregates[column_itr];
  }

  delete[] aggregates;
}

}  // namespace executor
}  // namespace peloton
