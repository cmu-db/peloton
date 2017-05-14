//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_insert_translator.h
//
// Identification: src/include/codegen/insert/abstract_insert_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/aggregation.h"
#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/oa_hash_table.h"
#include "codegen/operator_translator.h"
#include "codegen/updateable_storage.h"
#include "planner/insert_plan.h"

namespace peloton {
namespace codegen {

/**
 * @brief There are 3 different flavors of INSERT.
 *
 * - INSERT INTO TABLE_NAME (column1, column2, column3,...columnN)
 *   VALUES (value1, value2, value3,...valueN);
 *
 * - INSERT INTO TABLE_NAME
 *   VALUES (value1,value2,value3,...valueN);
 *
 * - INSERT INTO first_table_name [(column1, column2, ... columnN)]
 *     SELECT column1, column2, ...columnN
 *     FROM second_table_name
 *     [WHERE condition];
 */
class AbstractInsertTranslator : public OperatorTranslator {
 public:

  /**
   * @brief Constructor.
   */
  AbstractInsertTranslator(const planner::InsertPlan &insert_plan,
                           CompilationContext &context, Pipeline &pipeline);
  /**
   * @brief Codegen any initialization work for this operator.
   */
  void InitializeState() override {}

  /**
   * @brief Define any helper functions this translator needs
   */
  void DefineAuxiliaryFunctions() override {}

  /**
   * @brief Codegen produce new tuples.
   */
  void Produce() const override = 0;

  /**
   * @brief The method that consumes tuples from child operators.
   */
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override = 0;
  void Consume(ConsumerContext &context, RowBatch &batch) const override = 0;

  /**
   * @brief Codegen any cleanup work for this translator.
   */
  void TearDownState() override {}

  /**
   * @brief Get a stringified version of this translator.
   */
  std::string GetName() const override { return "Insert"; }

 protected:
  const planner::InsertPlan &insert_plan_;

};

}  // namespace codegen
}  // namespace peloton
