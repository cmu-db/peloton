#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/heap
#
# IDENTIFICATION
#    src/backend/access/heap/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/access/heap
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = heapam.o heap_fs.o heap_mm.o rel_block.o rel_block_table.o rel_block_fixed.o rel_block_varlen.o rel_block_utils.o hio.o pruneheap.o rewriteheap.o syncscan.o tuptoaster.o visibilitymap.o

include $(top_srcdir)/src/backend/common.mk
