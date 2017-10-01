//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_and_stats_calculator.h
//
// Identification: src/include/optimizer/cost_and_stats_calculator.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/stats/stats_storage.h"
#include "optimizer/cost_and_stats_calculator.h"
#include "optimizer/column_manager.h"
#include "optimizer/stats.h"
#include "optimizer/properties.h"
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/cost.h"
#include "type/types.h"

namespace peloton {
	namespace optimizer {
		//===----------------------------------------------------------------------===//
		// Helper functions
		//===----------------------------------------------------------------------===//
		// Generate output stats based on the input stats and the property column
		std::shared_ptr<TableStats> generateOutputStat(std::shared_ptr<TableStats> input_table_stats,
																									 const PropertyColumns* columns_prop) {
			std::vector<std::shared_ptr<ColumnStats>> output_column_stats;
			for (size_t i = 0; i < columns_prop->GetSize(); i++) {
				oid_t column_id = (oid_t)((expression::TupleValueExpression *)columns_prop->GetColumn(i).get())->GetColumnId();
				output_column_stats.push_back(input_table_stats->GetColumnStats(column_id));
			}
			return std::shared_ptr<TableStats>(new TableStats(input_table_stats->num_rows, output_column_stats));
		}

		// Update output stats num_rows for conjunctions based on two single conditions
		// TODO: move to cost.cpp
		void updateConjunctionStats(const std::shared_ptr<TableStats> &input_stats,
													 ValueCondition &lhs_cond, ValueCondition &rhs_cond, const ExpressionType type,
													 std::shared_ptr<TableStats> &output_stats) {
			std::shared_ptr<TableStats> lhs(new TableStats());
			std::shared_ptr<TableStats> rhs(new TableStats());
			Cost::UpdateConditionStats(input_stats, lhs_cond, lhs);
			Cost::UpdateConditionStats(input_stats, rhs_cond, rhs);
			Cost::CombineConjunctionStats(lhs, rhs, input_stats->num_rows, type, output_stats);
		}

		void CostAndStatsCalculator::CalculateCostAndStats(
			std::shared_ptr<GroupExpression> gexpr,
			const PropertySet *output_properties,
			const std::vector<PropertySet> *input_properties_list,
			std::vector<std::shared_ptr<Stats>> child_stats,
			std::vector<double> child_costs) {
			gexpr_ = gexpr;
			output_properties_ = output_properties;
			input_properties_list_ = input_properties_list;
			child_stats_ = child_stats;
			child_costs_ = child_costs;

			gexpr->Op().Accept(this);
		}

		void CostAndStatsCalculator::Visit(const DummyScan *) {
			// TODO: Replace with more accurate cost
			output_stats_.reset(new Stats(nullptr));
			output_cost_ = 0;
		}

		void CostAndStatsCalculator::Visit(const PhysicalSeqScan * op) {
			// TODO : Replace with more accurate cost
			// retrieve table_stats from catalog by db_id and table_id
			auto stats_storage = StatsStorage::GetInstance();
			auto table_stats = std::dynamic_pointer_cast<TableStats>(
				stats_storage->GetTableStats(op->table_->GetDatabaseOid(), op->table_->GetOid()));
      auto property_ = output_properties_->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>();
			auto output_stats = generateOutputStat(table_stats, property_);

			auto predicate_prop =
				output_properties_->GetPropertyOfType(PropertyType::PREDICATE)
					->As<PropertyPredicate>();
			if (predicate_prop == nullptr) {
				output_cost_ = Cost::NoConditionSeqScanCost(table_stats);
				output_stats_.reset(output_stats.get());
				return;
			}

			std::vector<oid_t> key_column_ids;
			std::vector<ExpressionType> expr_types;
			std::vector<type::Value> values;


			expression::AbstractExpression *predicate = predicate_prop->GetPredicate();
			bool canSearch;

			// traverse the predicate and retrieve the several tuple value expressions
			util::GetPredicateColumns(op->table_->GetSchema(), predicate,
													key_column_ids, expr_types,
													values, canSearch);

			if (key_column_ids.size() == 1) { // single condition
				ValueCondition condition(key_column_ids.at(0), expr_types.at(0), values.at(0));
				output_cost_ = Cost::SingleConditionSeqScanCost(table_stats, condition, output_stats);

			} else if (key_column_ids.size() == 2 &&
								 (predicate->GetExpressionType() == ExpressionType::CONJUNCTION_AND ||
									predicate->GetExpressionType() == ExpressionType::CONJUNCTION_OR)){ //conjunction expression
				ValueCondition lhs_cond(key_column_ids.at(0), expr_types.at(0), values.at(0));
				ValueCondition rhs_cond(key_column_ids.at(1), expr_types.at(1), values.at(1));
				//Get the scan_cost
				output_cost_ = Cost::NoConditionSeqScanCost(table_stats);
				//Update output stats num_rows
				updateConjunctionStats(table_stats, lhs_cond, rhs_cond, predicate->GetExpressionType(), output_stats);

			} else {

				// TODO: support multiple conjunctions
				output_cost_ = 2;
			}

		};

		void CostAndStatsCalculator::Visit(const PhysicalIndexScan *op) {
			// Simple cost function
			// indexSearchable ? Index : SeqScan
			// TODO : Replace with more accurate cost
			auto predicate_prop =
				output_properties_->GetPropertyOfType(PropertyType::PREDICATE)
					->As<PropertyPredicate>();

			if (predicate_prop == nullptr) {
				output_cost_ = 2;
				return;
			}

			std::vector<oid_t> key_column_ids;
			std::vector<ExpressionType> expr_types;
			std::vector<type::Value> values;
			oid_t index_id = 0;

			auto stats_storage = StatsStorage::GetInstance();
			auto table_stats = std::dynamic_pointer_cast<TableStats>(
				stats_storage->GetTableStats(op->table_->GetDatabaseOid(), op->table_->GetOid()));
			auto output_stats = generateOutputStat(table_stats, output_properties_
				->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>());
			expression::AbstractExpression *predicate = predicate_prop->GetPredicate();

			if (util::CheckIndexSearchable(op->table_, predicate, key_column_ids,
																		 expr_types, values, index_id)) {

				if (key_column_ids.size() == 1) {
					ValueCondition condition(key_column_ids.at(0), expr_types.at(0), values.at(0));
					output_cost_ = Cost::SingleConditionIndexScanCost(table_stats, condition, output_stats);

				} else if (key_column_ids.size() == 2 &&
					(predicate->GetExpressionType() == ExpressionType::CONJUNCTION_AND ||
					predicate->GetExpressionType() == ExpressionType::CONJUNCTION_OR)){
					ValueCondition lhs_cond(key_column_ids.at(0), expr_types.at(0), values.at(0));
					ValueCondition rhs_cond(key_column_ids.at(1), expr_types.at(1), values.at(1));
					//Just to get the index_cost+scan_cost
					output_cost_ = Cost::SingleConditionIndexScanCost(table_stats, lhs_cond, output_stats);
					//Update output stats num_rows
					updateConjunctionStats(table_stats, lhs_cond, rhs_cond, predicate->GetExpressionType(), output_stats);

				} else {

					// TODO: support multiple conjunctions
					output_cost_ = 0;
				}
			} else {
				if (key_column_ids.size() == 1) {
					ValueCondition condition(key_column_ids.at(0), expr_types.at(0), values.at(0));
					output_cost_ = Cost::SingleConditionSeqScanCost(table_stats, condition, output_stats);

				} else if (key_column_ids.size() == 2 &&
									 (predicate->GetExpressionType() == ExpressionType::CONJUNCTION_AND ||
										predicate->GetExpressionType() == ExpressionType::CONJUNCTION_OR)){
					ValueCondition lhs_cond(key_column_ids.at(0), expr_types.at(0), values.at(0));
					ValueCondition rhs_cond(key_column_ids.at(1), expr_types.at(1), values.at(1));
					//Just to get the index_cost+scan_cost
					output_cost_ = Cost::SingleConditionSeqScanCost(table_stats, lhs_cond, output_stats);
					//Update output stats num_rows
					updateConjunctionStats(table_stats, lhs_cond, rhs_cond, predicate->GetExpressionType(), output_stats);

				} else {

					// TODO: support multiple conjunctions
					output_cost_ = 2;
				}

			}
			output_stats_.reset(output_stats.get());
		};
		void CostAndStatsCalculator::Visit(const PhysicalProject *) {
			// TODO: Replace with more accurate cost
			PL_ASSERT(child_stats_.size() == 1);
			auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
			auto output_stats = generateOutputStat(table_stats_ptr, output_properties_
				->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>());
			output_cost_ = Cost::ProjectCost(std::dynamic_pointer_cast<TableStats>(child_stats_.at(0)), std::vector <oid_t>(),
																			 output_stats);
			output_stats_.reset(output_stats.get());
		}
		void CostAndStatsCalculator::Visit(const PhysicalOrderBy *) {
			// TODO: Replace with more accurate cost
			auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
			auto output_stats = generateOutputStat(table_stats_ptr, output_properties_
				->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>());
			auto sort_prop = std::dynamic_pointer_cast<PropertySort>(
				output_properties_->GetPropertyOfType(PropertyType::SORT));
			std::vector<oid_t> columns;
			std::vector<bool> orders;
			for(size_t i = 0; i < sort_prop->GetSortColumnSize(); i++) {
				oid_t column = (oid_t)((expression::TupleValueExpression *)sort_prop->GetSortColumn(i).get())->GetColumnId();
				columns.push_back(column);
				orders.push_back(sort_prop->GetSortAscending(i));
			}
			output_cost_ = Cost::OrderByCost(table_stats_ptr, columns, orders, output_stats);
			output_stats_.reset(output_stats.get());
		}
		void CostAndStatsCalculator::Visit(const PhysicalLimit *) {
			// TODO: Replace with more accurate cost
			// lack of limit number
			auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
			auto output_stats = generateOutputStat(table_stats_ptr, output_properties_
				->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>());
			size_t limit = (size_t) std::dynamic_pointer_cast<PropertyLimit>(
				output_properties_->GetPropertyOfType(PropertyType::LIMIT))->GetLimit();
			output_cost_ = Cost::LimitCost(std::dynamic_pointer_cast<TableStats>(child_stats_.at(0)),limit, output_stats);
			output_stats_.reset(output_stats.get());
		}
		void CostAndStatsCalculator::Visit(const PhysicalFilter *){};
		void CostAndStatsCalculator::Visit(const PhysicalInnerNLJoin *){
			// TODO: Replace with more accurate cost
			output_cost_ = 1;
		};
		void CostAndStatsCalculator::Visit(const PhysicalLeftNLJoin *){};
		void CostAndStatsCalculator::Visit(const PhysicalRightNLJoin *){};
		void CostAndStatsCalculator::Visit(const PhysicalOuterNLJoin *){};
		void CostAndStatsCalculator::Visit(const PhysicalInnerHashJoin *){
			// TODO: Replace with more accurate cost
			output_cost_ = 0;
		};
		void CostAndStatsCalculator::Visit(const PhysicalLeftHashJoin *){};
		void CostAndStatsCalculator::Visit(const PhysicalRightHashJoin *){};
		void CostAndStatsCalculator::Visit(const PhysicalOuterHashJoin *){};
		void CostAndStatsCalculator::Visit(const PhysicalInsert *) {
			// TODO: Replace with more accurate cost
			output_cost_ = 0;
		};
		void CostAndStatsCalculator::Visit(const PhysicalInsertSelect *) {
			// TODO: Replace with more accurate cost
			output_cost_ = 0;
		};
		void CostAndStatsCalculator::Visit(const PhysicalDelete *) {
			// TODO: Replace with more accurate cost
			output_cost_ = 0;
		};
		void CostAndStatsCalculator::Visit(const PhysicalUpdate *) {
			// TODO: Replace with more accurate cost
			output_cost_ = 0;
		};
		void CostAndStatsCalculator::Visit(const PhysicalHashGroupBy * op) {
			// TODO: Replace with more accurate cost
			PL_ASSERT(child_stats_.size() == 1);
			auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
			auto output_stats = generateOutputStat(table_stats_ptr, output_properties_
				->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>());
			std::vector<oid_t> column_ids;
			for (auto column: op->columns) {
				oid_t column_id = (oid_t)((expression::TupleValueExpression *)column.get())->GetColumnId();
				column_ids.push_back(column_id);
			}
			output_cost_ = Cost::HashGroupByCost(table_stats_ptr, column_ids, output_stats);
			output_stats_.reset(output_stats.get());
		};
		void CostAndStatsCalculator::Visit(const PhysicalSortGroupBy * op) {
			// TODO: Replace with more accurate cost
			PL_ASSERT(child_stats_.size() == 1);
			auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
			auto output_stats = generateOutputStat(table_stats_ptr,
																						 output_properties_->GetPropertyOfType(PropertyType::COLUMNS)
																							 ->As<PropertyColumns>());
			std::vector<oid_t> column_ids;
			for (auto column: op->columns) {
				oid_t column_id = (oid_t)((expression::TupleValueExpression *)column.get())->GetColumnId();
				column_ids.push_back(column_id);
			}
			output_cost_ = Cost::SortGroupByCost(table_stats_ptr, column_ids, output_stats);
			output_stats_.reset(output_stats.get());

		};
		void CostAndStatsCalculator::Visit(const PhysicalAggregate *) {
			// TODO: Replace with more accurate cost
			PL_ASSERT(child_stats_.size() == 1);
			output_cost_ = Cost::AggregateCost(std::dynamic_pointer_cast<TableStats>(child_stats_.at(0)));
			output_stats_.reset(child_stats_.at(0).get());
		};
		void CostAndStatsCalculator::Visit(const PhysicalDistinct *) {
			// TODO: Replace with more accurate cost
			PL_ASSERT(child_stats_.size() == 1);
			auto table_stats_ptr = std::dynamic_pointer_cast<TableStats>(child_stats_.at(0));
			auto output_stats = generateOutputStat(table_stats_ptr,
				output_properties_->GetPropertyOfType(PropertyType::COLUMNS)->As<PropertyColumns>());
			oid_t column_id = (oid_t)((expression::TupleValueExpression *)(
				output_properties_->GetPropertyOfType(PropertyType::DISTINCT)->As<PropertyDistinct>()
				->GetDistinctColumn(0)).get())->GetColumnId();

			output_cost_ = Cost::DistinctCost(table_stats_ptr, column_id, output_stats);
			output_stats_.reset(output_stats.get());
		};

	}  // namespace optimizer
}  // namespace peloton