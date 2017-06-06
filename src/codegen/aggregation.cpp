//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregation.cpp
//
// Identification: src/codegen/aggregation.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/aggregation.h"

#include "include/codegen/utils/if.h"
#include "codegen/type.h"
#include "common/logger.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Setup the aggregation storage format given the aggregates the caller wants to
// store.
//===----------------------------------------------------------------------===//
void Aggregation::Setup(
    CodeGen &codegen,
    const std::vector<planner::AggregatePlan::AggTerm> &aggregates) {
  // Collect the types so we can configure the updateable storage's format
  bool needs_group_count = false;
  bool tracks_group_count = false;
  for (uint32_t i = 0; i < aggregates.size(); i++) {
    const auto agg_term = aggregates[i];
    // Defense > Offense ?
    PL_ASSERT(agg_term.agg_ai.type != type::Type::TypeId::INVALID);

    needs_group_count |= agg_term.aggtype == ExpressionType::AGGREGATE_AVG;
    tracks_group_count |=
        agg_term.aggtype == ExpressionType::AGGREGATE_COUNT_STAR;
  }

  // Add the count(*) aggregate first, if we need to
  if (tracks_group_count || needs_group_count) {
    uint32_t storage_pos = storage_.AddType(type::Type::TypeId::BIGINT);
    AggregateInfo count_agg{
        ExpressionType::AGGREGATE_COUNT_STAR, type::Type::TypeId::BIGINT,
        std::numeric_limits<uint32_t>::max(), storage_pos, true};
    aggregate_infos_.push_back(count_agg);
  }

  // Add the rest
  for (uint32_t source_idx = 0; source_idx < aggregates.size(); source_idx++) {
    const auto agg_term = aggregates[source_idx];
    switch (agg_term.aggtype) {
      case ExpressionType::AGGREGATE_COUNT: {
        // We're counting instances ... use BIGINT for the count
        const auto count_type = type::Type::TypeId::BIGINT;
        uint32_t storage_pos = storage_.AddType(count_type);
        AggregateInfo aggregate_info{agg_term.aggtype, count_type, source_idx,
                                     storage_pos, false};
        aggregate_infos_.push_back(aggregate_info);
        break;
      }
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        // Count() - to check whether there is no item at all
        uint32_t count_storage_pos =
            storage_.AddType(type::Type::TypeId::BIGINT);
        AggregateInfo count_agg{ExpressionType::AGGREGATE_COUNT,
                                type::Type::TypeId::BIGINT, source_idx,
                                count_storage_pos, true};
        aggregate_infos_.push_back(count_agg);

        // Regular
        const auto value_type = agg_term.expression->GetValueType();
        uint32_t storage_pos = storage_.AddType(value_type);
        AggregateInfo aggregate_info{agg_term.aggtype, value_type, source_idx,
                                     storage_pos, false};
        aggregate_infos_.push_back(aggregate_info);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // We decompose averages (i.e., AVG(c)) into COUNT(c) and SUM(c)

        // SUM() - the type must match the type of the expression
        PL_ASSERT(agg_term.expression != nullptr);
        const auto sum_type = agg_term.expression->GetValueType();
        uint32_t sum_storage_pos = storage_.AddType(sum_type);
        AggregateInfo sum_agg{ExpressionType::AGGREGATE_SUM, sum_type,
                              source_idx, sum_storage_pos, true};
        aggregate_infos_.push_back(sum_agg);

        // COUNT() - can use big integer since we're counting instances
        uint32_t count_storage_pos =
            storage_.AddType(type::Type::TypeId::BIGINT);
        AggregateInfo count_agg{ExpressionType::AGGREGATE_COUNT,
                                type::Type::TypeId::BIGINT, source_idx,
                                count_storage_pos, true};
        aggregate_infos_.push_back(count_agg);

        // AVG()
        // TODO: Is this always double?
        AggregateInfo avg_agg{agg_term.aggtype, type::Type::TypeId::DECIMAL,
                              source_idx, std::numeric_limits<uint32_t>::max(),
                              false};
        aggregate_infos_.push_back(avg_agg);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // count(*)'s are always stored in the first slot, reference that here
        // NOTE: We've already added the count(*) to the storage format above,
        //       don't do it here
        PL_ASSERT(tracks_group_count);
        AggregateInfo count_agg{agg_term.aggtype, type::Type::TypeId::BIGINT,
                                source_idx, 0, false};
        aggregate_infos_.push_back(count_agg);
        break;
      }
      default: {
        std::string message = StringUtil::Format(
            "Unexpected aggregate type [%s] when preparing aggregator",
            ExpressionTypeToString(agg_term.aggtype).c_str());
        LOG_ERROR("%s", message.c_str());
        throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, message};
      }
    }
  }

  // Finalize the storage format
  storage_.Finalize(codegen);
}

// Create the initial values of all aggregates based on the the provided values
void Aggregation::CreateInitialValues(
    CodeGen &codegen, llvm::Value *storage_space,
    const std::vector<codegen::Value> &initial) const {
  // TODO: Handle null
  for (size_t i = 0; i < aggregate_infos_.size(); i++) {
    const auto &aggregate_info = aggregate_infos_[i];
    switch (aggregate_info.aggregate_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        // For the above aggregations, the initial value is the attribute value
        uint32_t val_source = aggregate_info.source_index;
        storage_.SetValueAt(codegen, storage_space,
                            aggregate_info.storage_index, initial[val_source]);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // TODO: Count(col) needs to check for null values
        // This aggregate should be moved to the front of the storage area
        storage_.SetValueAt(
            codegen, storage_space, aggregate_info.storage_index,
            codegen::Value{type::Type::TypeId::BIGINT, codegen.Const64(1)});
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // AVG() aggregates aren't physically stored
        break;
      }
      default: {
        std::string message = StringUtil::Format(
            "Unexpected aggregate type [%s] when creating initial values",
            ExpressionTypeToString(aggregate_info.aggregate_type).c_str());
        LOG_ERROR("%s", message.c_str());
        throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, message};
      }
    }
  }
}

//===----------------------------------------------------------------------===//
// Advance each of the aggregates stored in the provided storage space. We
// use the storage format class to determine the location of each of the
// aggregates. Then, depending on the type of aggregation, we advance of their
// values, populating the provided next_vals vector with their new values.
//===----------------------------------------------------------------------===//
void Aggregation::AdvanceValues(
    CodeGen &codegen, llvm::Value *storage_space,
    const std::vector<codegen::Value> &next_vals) const {
  // TODO: Handle null
  for (const auto &aggregate_info : aggregate_infos_) {
    // Loop over all aggregates, advancing each
    uint32_t source = aggregate_info.source_index;
    codegen::Value next;
    switch (aggregate_info.aggregate_type) {
      case ExpressionType::AGGREGATE_SUM: {
        PL_ASSERT(source < std::numeric_limits<uint32_t>::max());
        auto curr = storage_.GetValueAt(codegen, storage_space,
                                        aggregate_info.storage_index);
        next = curr.Add(codegen, next_vals[source]);
        break;
      }
      case ExpressionType::AGGREGATE_MIN: {
        PL_ASSERT(source < std::numeric_limits<uint32_t>::max());
        auto curr = storage_.GetValueAt(codegen, storage_space,
                                        aggregate_info.storage_index);
        next = curr.Min(codegen, next_vals[source]);
        break;
      }
      case ExpressionType::AGGREGATE_MAX: {
        PL_ASSERT(source < std::numeric_limits<uint32_t>::max());
        auto curr = storage_.GetValueAt(codegen, storage_space,
                                        aggregate_info.storage_index);
        next = curr.Max(codegen, next_vals[source]);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT: {
        PL_ASSERT(source < std::numeric_limits<uint32_t>::max());
        auto curr = storage_.GetValueAt(codegen, storage_space,
                                        aggregate_info.storage_index);
        auto delta = Value{type::Type::TypeId::BIGINT, codegen.Const64(1)};
        next = curr.Add(codegen, delta);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // COUNT(*) is always stored first with a source equal to INT_MAX. All
        // other COUNT(*)'s refer to that one.
        if (source != std::numeric_limits<uint32_t>::max()) {
          continue;
        }
        auto curr = storage_.GetValueAt(codegen, storage_space,
                                        aggregate_info.storage_index);
        auto delta = Value{type::Type::TypeId::BIGINT, codegen.Const64(1)};
        next = curr.Add(codegen, delta);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // AVG() aggregates aren't physically stored
        continue;
      }
      default: {
        std::string message = StringUtil::Format(
            "Unexpected aggregate type [%s] when advancing aggregator",
            ExpressionTypeToString(aggregate_info.aggregate_type).c_str());
        LOG_ERROR("%s", message.c_str());
        throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, message};
      }
    }

    // StoreValue the next value in the appropriate slot
    PL_ASSERT(next.GetType() != type::Type::TypeId::INVALID);
    storage_.SetValueAt(codegen, storage_space, aggregate_info.storage_index,
                        next);
  }
}

//===----------------------------------------------------------------------===//
// This function will finalize the aggregates stored in the provided storage
// space.  Finalization essentially means computing the final values of the
// aggregates. This is only really necessary for averages. Either way, we
// populate the final_vals vector with the final values of all the aggregates.
//===----------------------------------------------------------------------===//
void Aggregation::FinalizeValues(
    CodeGen &codegen, llvm::Value *storage_space,
    std::vector<codegen::Value> &final_vals) const {
  // Collect all final values into this map. We need this because some
  // aggregates are derived from other component aggregates.
  std::map<std::pair<uint32_t, ExpressionType>, codegen::Value> vals;

  for (const auto &aggregate_info : aggregate_infos_) {
    uint32_t source = aggregate_info.source_index;
    ExpressionType agg_type = aggregate_info.aggregate_type;
    switch (agg_type) {
      case ExpressionType::AGGREGATE_COUNT: {
        codegen::Value final_val = storage_.GetValueAt(
            codegen, storage_space, aggregate_info.storage_index);
        vals[std::make_pair(source, agg_type)] = final_val;
        if (!aggregate_info.is_internal) {
          final_vals.push_back(final_val);
        }
        break;
      }
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        codegen::Value count = vals[{source, ExpressionType::AGGREGATE_COUNT}];
        codegen::Value final_calc = storage_.GetValueAt(
            codegen, storage_space, aggregate_info.storage_index);

        vals[std::make_pair(source, agg_type)] = final_calc;
        if (!aggregate_info.is_internal) {
          // Empty check
          codegen::Value zero{type::Type::TypeId::INTEGER, codegen.Const32(0)};
          codegen::Value zero_count = count.CompareEq(codegen, zero);

          codegen::Value final_null;
          If check_count{codegen, zero_count.GetValue()};
          {
            final_null = Type::GetNullValue(codegen, final_calc.GetType());
          }
          check_count.EndIf();
          auto final_val = check_count.BuildPHI(final_null, final_calc);
          final_vals.push_back(final_val);
        }
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // Find the sum and count for this aggregate
        codegen::Value count = vals[{source, ExpressionType::AGGREGATE_COUNT}];
        count = count.CastTo(codegen, type::Type::TypeId::DECIMAL);

        codegen::Value sum = vals[{source, ExpressionType::AGGREGATE_SUM}];
        sum = sum.CastTo(codegen, type::Type::TypeId::DECIMAL);

        codegen::Value final_val =
            sum.Div(codegen, count, Value::OnError::ReturnNull);

        vals[std::make_pair(source, agg_type)] = final_val;
        final_vals.push_back(final_val);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        if (source != std::numeric_limits<uint32_t>::max()) {
          PL_ASSERT(!aggregate_info.is_internal);
          uint32_t count_source = std::numeric_limits<uint32_t>::max();
          final_vals.push_back(vals[std::make_pair(count_source, agg_type)]);
        } else {
          PL_ASSERT(aggregate_info.is_internal);
          codegen::Value final_val = storage_.GetValueAt(
              codegen, storage_space, aggregate_info.storage_index);
          vals[std::make_pair(source, agg_type)] = final_val;
        }
        break;
      }
      default: {
        std::string message = StringUtil::Format(
            "Unexpected aggregate type [%s] when finalizing aggregator",
            ExpressionTypeToString(agg_type).c_str());
        LOG_ERROR("%s", message.c_str());
        throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, message};
      }
    }
  }
}

}  // namespace codegen
}  // namespace peloton
