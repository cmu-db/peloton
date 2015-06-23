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
				  order_by_test

executor_tests_common= 	executor/executor_tests_util.cpp \
						harness.cpp

executor_tests_common_ld = \
						   $(obj_DIR)/catalog/manager.o \
						   $(obj_DIR)/catalog/schema.o \
						   $(obj_DIR)/common/types.o \
						   $(obj_DIR)/common/value.o \
						   $(obj_DIR)/common/varlen.o \
						   $(obj_DIR)/common/pool.o \
						   $(obj_DIR)/concurrency/transaction_manager.o \
						   $(obj_DIR)/concurrency/transaction.o \
						   $(obj_DIR)/storage/abstract_table.o \
						   $(obj_DIR)/storage/data_table.o \
						   $(obj_DIR)/storage/table_factory.o \
						   $(obj_DIR)/storage/tile_group.o \
						   $(obj_DIR)/storage/tile_group_header.o \
						   $(obj_DIR)/storage/tile_group_factory.o \
						   $(obj_DIR)/storage/tile.o \
						   $(obj_DIR)/storage/tuple.o \
						   $(obj_DIR)/index/index.o \
						   $(obj_DIR)/index/index_factory.o \
						   $(obj_DIR)/index/concurrent_btree.o \
						   $(obj_DIR)/index/btree_index.o \
						   $(obj_DIR)/executor/abstract_executor.o \
						   $(obj_DIR)/executor/logical_tile.o \
						   $(obj_DIR)/executor/logical_tile_factory.o \
						   $(obj_DIR)/planner/abstract_plan_node.o \
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
							 $(obj_DIR)/executor/materialization_executor.o

mutate_test_SOURCES = \
					  $(executor_tests_common) \
					  executor/mutate_test.cpp 

mutate_test_LDADD = \
					$(executor_tests_common_ld) \
					$(obj_DIR)/expression/abstract_expression.o \
					$(obj_DIR)/expression/expression_util.o \
					$(obj_DIR)/executor/seq_scan_executor.o \
					$(obj_DIR)/executor/delete_executor.o \
					$(obj_DIR)/executor/insert_executor.o \
					$(obj_DIR)/executor/update_executor.o

seq_scan_test_SOURCES = \
						$(executor_tests_common) \
						executor/seq_scan_test.cpp 

seq_scan_test_LDADD = \
					  $(executor_tests_common_ld) \
					  $(obj_DIR)/expression/abstract_expression.o \
					  $(obj_DIR)/expression/expression_util.o \
					  $(obj_DIR)/executor/seq_scan_executor.o

index_scan_test_SOURCES = \
						  $(executor_tests_common) \
						  executor/index_scan_test.cpp 

index_scan_test_LDADD = \
						$(executor_tests_common_ld) \
						$(obj_DIR)/executor/index_scan_executor.o

limit_test_SOURCES = \
					 $(executor_tests_common) \
					 executor/limit_test.cpp

limit_test_LDADD = \
				   $(executor_tests_common_ld) \
				   $(obj_DIR)/executor/limit_executor.o

nested_loop_join_test_SOURCES = \
								$(executor_tests_common) \
								executor/nested_loop_join_test.cpp 

nested_loop_join_test_LDADD = \
							  $(executor_tests_common_ld) \
							  $(obj_DIR)/expression/abstract_expression.o \
							  $(obj_DIR)/expression/expression_util.o \
							  $(obj_DIR)/executor/nested_loop_join_executor.o

order_by_test_SOURCES = \
						$(executor_tests_common) \
						executor/order_by_test.cpp 

order_by_test_LDADD = \
					$(executor_tests_common_ld) \
					$(obj_DIR)/executor/order_by_executor.o
