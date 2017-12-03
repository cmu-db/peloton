//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_translator.h
//
// Identification: src/include/codegen/operator/insert_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/table_storage.h"

namespace peloton {

namespace planner {
class InsertPlan;
}  // namespace planner

namespace codegen {

/**
 * @brief There are 4 different flavors of INSERT.
 *
 * - INSERT INTO TABLE_NAME
 *   VALUES (value1,value2,value3,...valueN);
 *
 * - INSERT INTO TABLE_NAME (column1, column2, column3,...columnN)
 *   VALUES (value1, value2, value3,...valueN);
 *
 * - INSERT INTO TABLE_NAME
 *   VALUES (val1-a,val2-a,...valN-a), (val1-b, val2-b, ...valN-b), ...
 *          (val1-M,val2-M,...valN-M);
 *
 * - INSERT INTO first_table_name [(column1, column2, ... columnN)]
 *     SELECT column1, column2, ...columnN
 *     FROM second_table_name
 *     [WHERE condition];
 */
class InsertTranslator : public OperatorTranslator {
 public:
  // Constructor.
  InsertTranslator(const planner::InsertPlan &insert_plan,
                   CompilationContext &context, Pipeline &pipeline);

  // Codegen any initialization work
  void InitializeState() override;

  // Define any helper function for this translator needs
  void DefineAuxiliaryFunctions() override {}

  // Codegen produce
  void Produce() const override;

  // Codegen consuming a row tuple from child operators
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  // Codegen any cleanup work
  void TearDownState() override;

  // Get the name of this trnslator
  std::string GetName() const override { return "Insert"; }

 private:
  const planner::InsertPlan &insert_plan_;

  RuntimeState::StateID inserter_state_id_;

  codegen::TableStorage table_storage_;
};

}  // namespace codegen
}  // namespace peloton
