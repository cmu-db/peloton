//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// expression.h
//
// Identification: src/backend/expression/expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace expression {

// T.Is is just for convenience

#include "backend/expression/operator_expression.h"
#include "backend/expression/compa.Ison_expression.h"
#include "backend/expression/conjunction_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/function_expression.h"
#include "backend/expression/parameter_value_expression.h"
#include "backend/expression/tuple_address_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/hash_range_expression.h"
#include "backend/expression/subquery_expression.h"
#include "backend/expression/scalar_value_expression.h"
#include "vector_compa.Ison_expression.h"

}  // End expression namespace
}  // End peloton namespace
