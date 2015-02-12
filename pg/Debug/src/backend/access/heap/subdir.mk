################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/access/heap/heap_fs.o \
../src/backend/access/heap/heap_manager.o \
../src/backend/access/heap/heap_mm.o \
../src/backend/access/heap/heap_vm.o \
../src/backend/access/heap/heapam.o \
../src/backend/access/heap/hio.o \
../src/backend/access/heap/pruneheap.o \
../src/backend/access/heap/rel_block.o \
../src/backend/access/heap/rel_block_fixed.o \
../src/backend/access/heap/rel_block_table.o \
../src/backend/access/heap/rel_block_utils.o \
../src/backend/access/heap/rel_block_varlen.o \
../src/backend/access/heap/relblock.o \
../src/backend/access/heap/relblock_fixed.o \
../src/backend/access/heap/relblock_table.o \
../src/backend/access/heap/relblock_utils.o \
../src/backend/access/heap/relblock_varlen.o \
../src/backend/access/heap/rewriteheap.o \
../src/backend/access/heap/syncscan.o \
../src/backend/access/heap/tuptoaster.o \
../src/backend/access/heap/visibilitymap.o 

C_SRCS += \
../src/backend/access/heap/heap_fs.c \
../src/backend/access/heap/heap_mm.c \
../src/backend/access/heap/heapam.c \
../src/backend/access/heap/hio.c \
../src/backend/access/heap/pruneheap.c \
../src/backend/access/heap/rel_block.c \
../src/backend/access/heap/rel_block_fixed.c \
../src/backend/access/heap/rel_block_table.c \
../src/backend/access/heap/rel_block_utils.c \
../src/backend/access/heap/rel_block_varlen.c \
../src/backend/access/heap/rewriteheap.c \
../src/backend/access/heap/syncscan.c \
../src/backend/access/heap/tuptoaster.c \
../src/backend/access/heap/visibilitymap.c 

OBJS += \
./src/backend/access/heap/heap_fs.o \
./src/backend/access/heap/heap_mm.o \
./src/backend/access/heap/heapam.o \
./src/backend/access/heap/hio.o \
./src/backend/access/heap/pruneheap.o \
./src/backend/access/heap/rel_block.o \
./src/backend/access/heap/rel_block_fixed.o \
./src/backend/access/heap/rel_block_table.o \
./src/backend/access/heap/rel_block_utils.o \
./src/backend/access/heap/rel_block_varlen.o \
./src/backend/access/heap/rewriteheap.o \
./src/backend/access/heap/syncscan.o \
./src/backend/access/heap/tuptoaster.o \
./src/backend/access/heap/visibilitymap.o 

C_DEPS += \
./src/backend/access/heap/heap_fs.d \
./src/backend/access/heap/heap_mm.d \
./src/backend/access/heap/heapam.d \
./src/backend/access/heap/hio.d \
./src/backend/access/heap/pruneheap.d \
./src/backend/access/heap/rel_block.d \
./src/backend/access/heap/rel_block_fixed.d \
./src/backend/access/heap/rel_block_table.d \
./src/backend/access/heap/rel_block_utils.d \
./src/backend/access/heap/rel_block_varlen.d \
./src/backend/access/heap/rewriteheap.d \
./src/backend/access/heap/syncscan.d \
./src/backend/access/heap/tuptoaster.d \
./src/backend/access/heap/visibilitymap.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/access/heap/%.o: ../src/backend/access/heap/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


