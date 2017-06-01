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

#include "codegen/if.h"
#include "codegen/type.h"
#include "common/logger.h"

namespace peloton {
namespace codegen {

// TODO: Handle shared/duplicated aggregates

//===----------------------------------------------------------------------===//
// Setup the aggregation storage format given the aggregates the caller wants to
// store.
//===----------------------------------------------------------------------===//
void Aggregation::Setup(
    CodeGen &codegen,
    const std::vector<planner::AggregatePlan::AggTerm> &aggregates) {
  // Add the rest
  for (uint32_t source_idx = 0; source_idx < aggregates.size(); source_idx++) {
    const auto agg_term = aggregates[source_idx];
    switch (agg_term.aggtype) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // Add the count to the storage layout
        const auto count_type = type::Type::TypeId::BIGINT;
        uint32_t storage_pos = storage_.AddType(count_type);

        // Add metadata for the count aggregate
        AggregateInfo aggregate_info{agg_term.aggtype, count_type, source_idx,
                                     storage_pos, false};
        aggregate_infos_.push_back(aggregate_info);
        break;
      }
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        // Add the element to the storage layout
        const auto value_type = agg_term.expression->GetValueType();
        uint32_t storage_pos = storage_.AddType(value_type);

        // Add metadata for the aggregate
        AggregateInfo aggregate_info{agg_term.aggtype, value_type, source_idx,
                                     storage_pos, false};
        aggregate_infos_.push_back(aggregate_info);
        break;
      }
      case ExpressionType::AGGREGATE_AVG: {
        // We decompose averages into separate COUNT() and SUM() components

        // SUM() - the type must match the type of the expression
        // First add the sum to the storage layout
        PL_ASSERT(agg_term.expression != nullptr);
        const auto sum_type = agg_term.expression->GetValueType();
        uint32_t sum_storage_pos = storage_.AddType(sum_type);

        // Add metadata about the SUM() aggregate
        AggregateInfo sum_agg{ExpressionType::AGGREGATE_SUM, sum_type,
                              source_idx, sum_storage_pos, true};
        aggregate_infos_.push_back(sum_agg);

        // COUNT() - can use big integer since we're counting instances
        // First add the count to the storage layout
        uint32_t count_storage_pos =
            storage_.AddType(type::Type::TypeId::BIGINT);

        // Add metadata about the COUNT() aggregate
        AggregateInfo count_agg{ExpressionType::AGGREGATE_COUNT,
                                type::Type::TypeId::BIGINT, source_idx,
                                count_storage_pos, true};
        aggregate_infos_.push_back(count_agg);

        // AVG() - this isn't storage physically, but we need metadata about it
        // TODO: Is this always double?
        AggregateInfo avg_agg{agg_term.aggtype, type::Type::TypeId::DECIMAL,
                              source_idx, source_idx, false};
        aggregate_infos_.push_back(avg_agg);
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
  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, storage_space};

  // Initialize bitmap to all NULLs
  null_bitmap.InitAllNull(codegen);

  for (size_t i = 0; i < aggregate_infos_.size(); i++) {
    const auto &aggregate_info = aggregate_infos_[i];
    switch (aggregate_info.aggregate_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        // For the above aggregations, the initial value is the attribute value
        uint32_t val_source = aggregate_info.source_index;
        storage_.SetValue(codegen, storage_space, aggregate_info.storage_index,
                          initial[val_source], null_bitmap);
        break;
      }
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // The initial value for count's are 1 (i.e., this row)
        codegen::Value one{type::Type::TypeId::BIGINT, codegen.Const64(1)};
        storage_.SetValue(codegen, storage_space, aggregate_info.storage_index,
                          one, null_bitmap);
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

  // Write the final contents of the null bitmap
  null_bitmap.WriteBack(codegen);
}

void Aggregation::DoAdvanceValue(
    CodeGen &codegen, llvm::Value *storage_space,
    const Aggregation::AggregateInfo &aggregate_info,
    const codegen::Value &update) const {
  codegen::Value next;
  switch (aggregate_info.aggregate_type) {
    case ExpressionType::AGGREGATE_SUM: {
      auto curr = storage_.GetValueSkipNull(codegen, storage_space,
                                            aggregate_info.storage_index);
      next = curr.Add(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_MIN: {
      auto curr = storage_.GetValueSkipNull(codegen, storage_space,
                                            aggregate_info.storage_index);
      next = curr.Min(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_MAX: {
      auto curr = storage_.GetValueSkipNull(codegen, storage_space,
                                            aggregate_info.storage_index);
      next = curr.Max(codegen, update);
      break;
    }
    case ExpressionType::AGGREGATE_COUNT: {
      auto curr = storage_.GetValueSkipNull(codegen, storage_space,
                                            aggregate_info.storage_index);
      auto delta = Value{type::Type::TypeId::BIGINT, codegen.Const64(1)};
      next = curr.Add(codegen, delta);
      break;
    }
    case ExpressionType::AGGREGATE_COUNT_STAR: {
      auto curr = storage_.GetValueSkipNull(codegen, storage_space,
                                            aggregate_info.storage_index);
      auto delta = Value{type::Type::TypeId::BIGINT, codegen.Const64(1)};
      next = curr.Add(codegen, delta);
      break;
    }
    case ExpressionType::AGGREGATE_AVG: {
      // AVG() aggregates aren't physically stored
      return;
    }
    default: {
      std::string message = StringUtil::Format(
          "Unexpected aggregate type [%s] when advancing aggregator",
          ExpressionTypeToString(aggregate_info.aggregate_type).c_str());
      LOG_ERROR("%s", message.c_str());
      throw Exception{EXCEPTION_TYPE_UNKNOWN_TYPE, message};
    }
  }

  // Store the updated value in the appropriate slot
  PL_ASSERT(next.GetType() != type::Type::TypeId::INVALID);
  storage_.SetValueSkipNull(codegen, storage_space,
                            aggregate_info.storage_index, next);
}

//===----------------------------------------------------------------------===//
// Advance each of the aggregates stored in the provided storage space
//===----------------------------------------------------------------------===//
void Aggregation::AdvanceValues(
    CodeGen &codegen, llvm::Value *storage_space,
    const std::vector<codegen::Value> &next_vals) const {
  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, storage_space};

  // Loop over all aggregates, advancing each
  for (const auto &aggregate_info : aggregate_infos_) {
    if (aggregate_info.aggregate_type == ExpressionType::AGGREGATE_AVG) {
      // AVG() aggregates are metadata only
      continue;
    }

    const Value &update = next_vals[aggregate_info.source_index];

    if (!null_bitmap.IsNullable(aggregate_info.storage_index) ||
        aggregate_info.aggregate_type == ExpressionType::AGGREGATE_COUNT_STAR) {
      // This aggregate is not nullable, generate fast path
      DoAdvanceValue(codegen, storage_space, aggregate_info, update);
    } else {
      // This aggregate is NULL-able, we need to check if the update value is
      // NULL, and whether the current value of the aggregate is NULL.

      // There are two cases we handle:
      // (1). If neither the update value or the current aggregate value are
      //      NULL, we simple do the regular aggregation without NULL checking.
      // (2). If the update value is not NULL, but the current aggregate **is**
      //      NULL, then we just store this update value as if we're creating it
      //      for the first time.
      //
      // If either the update value or the current aggregate are NULL, we have
      // nothing to do.

      llvm::Value *update_not_null = update.IsNotNull(codegen);
      llvm::Value *agg_null =
          null_bitmap.IsNull(codegen, aggregate_info.storage_index);

      llvm::Value *curr_val =
          null_bitmap.ByteFor(codegen, aggregate_info.storage_index);

      If valid_update{codegen, update_not_null};
      {
        If agg_is_null{codegen, agg_null};
        {
          // (2)
          switch (aggregate_info.aggregate_type) {
            case ExpressionType::AGGREGATE_SUM:
            case ExpressionType::AGGREGATE_MIN:
            case ExpressionType::AGGREGATE_MAX: {
              storage_.SetValue(codegen, storage_space,
                                aggregate_info.storage_index, update,
                                null_bitmap);
              break;
            }
            case ExpressionType::AGGREGATE_COUNT: {
              codegen::Value one{type::Type::TypeId::BIGINT,
                                 codegen.Const64(1)};
              storage_.SetValue(codegen, storage_space,
                                aggregate_info.storage_index, one, null_bitmap);
              break;
            }
            default: { break; }
          }
        }
        agg_is_null.ElseBlock();
        {
          // (1)
          DoAdvanceValue(codegen, storage_space, aggregate_info, update);
        }
        agg_is_null.EndIf();

        // Merge the null value
        null_bitmap.MergeValues(agg_is_null, curr_val);
      }
      valid_update.EndIf();

      // Merge the null value
      null_bitmap.MergeValues(valid_update, curr_val);
    }
  }

  // Write the final contents of the null bitmap
  null_bitmap.WriteBack(codegen);
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

  // The null bitmap tracker
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_, storage_space};

  for (const auto &aggregate_info : aggregate_infos_) {
    uint32_t source = aggregate_info.source_index;
    ExpressionType agg_type = aggregate_info.aggregate_type;
    switch (agg_type) {
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX: {
        codegen::Value final_val = storage_.GetValue(
            codegen, storage_space, aggregate_info.storage_index, null_bitmap);

        vals[std::make_pair(source, agg_type)] = final_val;
        if (!aggregate_info.is_internal) {
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
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_COUNT_STAR: {
        // COUNT(*) can never return NULL
        codegen::Value final_val = storage_.GetValueSkipNull(
            codegen, storage_space, aggregate_info.storage_index);

        vals[std::make_pair(source, agg_type)] = final_val;
        if (!aggregate_info.is_internal) {
          final_vals.push_back(final_val);
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
