## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# EXECUTOR
######################################################################

check_PROGRAMS += \
				  logical_tile_test \
				  materialization_test \
				  mutate_test \
				  seq_scan_test \
				  index_scan_test \
				  limit_test \
				  nested_loop_join_test \
				  order_by_test \
				  hash_set_op_test \
				  aggregate_test \
				  append_test

executor_tests_common= 	executor/executor_tests_util.cpp \
						harness.cpp

executor_tests_common_ld = \
						   $(obj_DIR)/catalog/libpeloton_la-manager.lo \
						   $(obj_DIR)/catalog/libpeloton_la-schema.lo \
						   $(obj_DIR)/common/libpeloton_la-types.lo \
						   $(obj_DIR)/common/libpeloton_la-value.lo \
						   $(obj_DIR)/common/libpeloton_la-varlen.lo \
						   $(obj_DIR)/common/libpeloton_la-pool.lo \
						   $(obj_DIR)/concurrency/libpeloton_la-transaction_manager.lo \
						   $(obj_DIR)/concurrency/libpeloton_la-transaction.lo \
						   $(obj_DIR)/storage/libpeloton_la-abstract_table.lo \
						   $(obj_DIR)/storage/libpeloton_la-data_table.lo \
						   $(obj_DIR)/storage/libpeloton_la-table_factory.lo \
						   $(obj_DIR)/storage/libpeloton_la-tile_group.lo \
						   $(obj_DIR)/storage/libpeloton_la-tile_group_header.lo \
						   $(obj_DIR)/storage/libpeloton_la-tile_group_factory.lo \
						   $(obj_DIR)/storage/libpeloton_la-tile.lo \
						   $(obj_DIR)/storage/libpeloton_la-tuple.lo \
						   $(obj_DIR)/index/libpeloton_la-index.lo \
						   $(obj_DIR)/index/libpeloton_la-index_factory.lo \
						   $(obj_DIR)/index/libpeloton_la-concurrent_btree.lo \
						   $(obj_DIR)/index/libpeloton_la-btree_index.lo \
						   $(obj_DIR)/executor/libpeloton_la-abstract_executor.lo \
						   $(obj_DIR)/executor/libpeloton_la-logical_tile.lo \
						   $(obj_DIR)/executor/libpeloton_la-logical_tile_factory.lo \
						   $(obj_DIR)/planner/libpeloton_la-abstract_plan_node.lo \
						   $(top_builddir)/src/libpeloton.la

logical_tile_test_SOURCES = \
							$(executor_tests_common) \
							executor/logical_tile_test.cpp 

logical_tile_test_LDADD = \
						  $(executor_tests_common_ld)

materialization_test_SOURCES = \
							   $(executor_tests_common) \
							   executor/materialization_test.cpp 

materialization_test_LDADD = \
							 $(executor_tests_common_ld) \
							 $(obj_DIR)/executor/libpeloton_la-materialization_executor.lo

mutate_test_SOURCES = \
					  $(executor_tests_common) \
					  executor/mutate_test.cpp 

mutate_test_LDADD = \
					$(executor_tests_common_ld) \
					$(obj_DIR)/expression/libpeloton_la-abstract_expression.lo \
					$(obj_DIR)/expression/libpeloton_la-expression_util.lo \
					$(obj_DIR)/executor/libpeloton_la-seq_scan_executor.lo \
					$(obj_DIR)/executor/libpeloton_la-delete_executor.lo \
					$(obj_DIR)/executor/libpeloton_la-insert_executor.lo \
					$(obj_DIR)/executor/libpeloton_la-update_executor.lo

seq_scan_test_SOURCES = \
						$(executor_tests_common) \
						executor/seq_scan_test.cpp 

seq_scan_test_LDADD = \
					  $(executor_tests_common_ld) \
					  $(obj_DIR)/expression/libpeloton_la-abstract_expression.lo \
					  $(obj_DIR)/expression/libpeloton_la-expression_util.lo \
					  $(obj_DIR)/executor/libpeloton_la-seq_scan_executor.lo

index_scan_test_SOURCES = \
						  $(executor_tests_common) \
						  executor/index_scan_test.cpp 

index_scan_test_LDADD = \
						$(executor_tests_common_ld) \
						$(obj_DIR)/executor/libpeloton_la-index_scan_executor.lo

limit_test_SOURCES = \
					 $(executor_tests_common) \
					 executor/limit_test.cpp

limit_test_LDADD = \
				   $(executor_tests_common_ld) \
				   $(obj_DIR)/executor/libpeloton_la-limit_executor.lo

nested_loop_join_test_SOURCES = \
								$(executor_tests_common) \
								executor/nested_loop_join_test.cpp 

nested_loop_join_test_LDADD = \
							  $(executor_tests_common_ld) \
							  $(obj_DIR)/expression/libpeloton_la-abstract_expression.lo \
							  $(obj_DIR)/expression/libpeloton_la-expression_util.lo \
							  $(obj_DIR)/executor/libpeloton_la-nested_loop_join_executor.lo

order_by_test_SOURCES = \
						$(executor_tests_common) \
						executor/order_by_test.cpp 

order_by_test_LDADD = \
					$(executor_tests_common_ld) \
					$(obj_DIR)/executor/libpeloton_la-order_by_executor.lo
					
hash_set_op_test_SOURCES = \
						$(executor_tests_common) \
						executor/hash_set_op_test.cpp
						
hash_set_op_test_LDADD = \
						$(executor_tests_common_ld) \
						$(obj_DIR)/executor/libpeloton_la-hash_set_op_executor.lo						

aggregate_test_SOURCES = \
						$(executor_tests_common) \
						executor/aggregate_test.cpp 

aggregate_test_LDADD = \
					$(executor_tests_common_ld) \
					$(obj_DIR)/executor/libpeloton_la-aggregator.lo \
					$(obj_DIR)/executor/libpeloton_la-aggregate_executor.lo
					
append_test_SOURCES = \
					$(executor_tests_common) \
					executor/append_test.cpp
					
append_test_LDADD = \
					$(executor_tests_common_ld) \
					$(obj_DIR)/executor/libpeloton_la-append_executor.lo
					
