//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_optimizer.h
//
// Identification: src/include/optimizer/abstract_optmizer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/printable.h"

namespace peloton{
namespace optimizer{
	class AbstractOptimizer : public Printable {

		public:
		AbstractOptimizer(const AbstractOptimizer &) = delete;
		AbstractOptimizer &operator=(const AbstractOptimizer &) = delete;
		AbstractOptimizer(AbstractOptimizer &&) = delete;
		AbstractOptimizer &operator=(AbstractOptimizer &&) = delete;

		AbstractOptimizer();

	};
} // namespace optimizer
} // namespace peloton
