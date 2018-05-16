//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// export_external_file_plan.h
//
// Identification: src/include/planner/export_external_file_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>
#include <string>

#include "planner/abstract_plan.h"

namespace peloton {
namespace planner {

/**
 * This is the plan node when exporting data from the database into an external
 * file. It is configured with the name of the file to write content into, and
 * the delimiter, quote, and escape characters to use when writing content.
 */
class ExportExternalFilePlan : public AbstractPlan {
 public:
  ExportExternalFilePlan(std::string file_name, char delimiter = ',',
                         char quote = '"', char escape = '\"');

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  PlanNodeType GetPlanNodeType() const override;

  const std::string &GetFileName() const { return file_name_; }

  char GetDelimiterChar() const { return delimiter_; }
  char GetQuoteChar() const { return quote_; }
  char GetEscapeChar() const { return escape_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Utilities + Internal
  ///
  //////////////////////////////////////////////////////////////////////////////

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;

  std::unique_ptr<AbstractPlan> Copy() const override;

  void PerformBinding(BindingContext &binding_context) override;

 private:
  std::vector<const planner::AttributeInfo *> output_attributes_;

  std::string file_name_;

  char delimiter_;
  char quote_;
  char escape_;
};

}  // namespace planner
}  // namespace peloton