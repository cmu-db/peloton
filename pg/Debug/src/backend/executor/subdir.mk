################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/executor/execAmi.o \
../src/backend/executor/execCurrent.o \
../src/backend/executor/execGrouping.o \
../src/backend/executor/execJunk.o \
../src/backend/executor/execMain.o \
../src/backend/executor/execProcnode.o \
../src/backend/executor/execQual.o \
../src/backend/executor/execScan.o \
../src/backend/executor/execTuples.o \
../src/backend/executor/execUtils.o \
../src/backend/executor/functions.o \
../src/backend/executor/instrument.o \
../src/backend/executor/nodeAgg.o \
../src/backend/executor/nodeAppend.o \
../src/backend/executor/nodeBitmapAnd.o \
../src/backend/executor/nodeBitmapHeapscan.o \
../src/backend/executor/nodeBitmapIndexscan.o \
../src/backend/executor/nodeBitmapOr.o \
../src/backend/executor/nodeCtescan.o \
../src/backend/executor/nodeCustom.o \
../src/backend/executor/nodeForeignscan.o \
../src/backend/executor/nodeFunctionscan.o \
../src/backend/executor/nodeGroup.o \
../src/backend/executor/nodeHash.o \
../src/backend/executor/nodeHashjoin.o \
../src/backend/executor/nodeIndexonlyscan.o \
../src/backend/executor/nodeIndexscan.o \
../src/backend/executor/nodeLimit.o \
../src/backend/executor/nodeLockRows.o \
../src/backend/executor/nodeMaterial.o \
../src/backend/executor/nodeMergeAppend.o \
../src/backend/executor/nodeMergejoin.o \
../src/backend/executor/nodeModifyTable.o \
../src/backend/executor/nodeNestloop.o \
../src/backend/executor/nodeRecursiveunion.o \
../src/backend/executor/nodeResult.o \
../src/backend/executor/nodeSeqscan.o \
../src/backend/executor/nodeSetOp.o \
../src/backend/executor/nodeSort.o \
../src/backend/executor/nodeSubplan.o \
../src/backend/executor/nodeSubqueryscan.o \
../src/backend/executor/nodeTidscan.o \
../src/backend/executor/nodeUnique.o \
../src/backend/executor/nodeValuesscan.o \
../src/backend/executor/nodeWindowAgg.o \
../src/backend/executor/nodeWorktablescan.o \
../src/backend/executor/spi.o \
../src/backend/executor/tstoreReceiver.o 

C_SRCS += \
../src/backend/executor/execAmi.c \
../src/backend/executor/execCurrent.c \
../src/backend/executor/execGrouping.c \
../src/backend/executor/execJunk.c \
../src/backend/executor/execMain.c \
../src/backend/executor/execProcnode.c \
../src/backend/executor/execQual.c \
../src/backend/executor/execScan.c \
../src/backend/executor/execTuples.c \
../src/backend/executor/execUtils.c \
../src/backend/executor/functions.c \
../src/backend/executor/instrument.c \
../src/backend/executor/nodeAgg.c \
../src/backend/executor/nodeAppend.c \
../src/backend/executor/nodeBitmapAnd.c \
../src/backend/executor/nodeBitmapHeapscan.c \
../src/backend/executor/nodeBitmapIndexscan.c \
../src/backend/executor/nodeBitmapOr.c \
../src/backend/executor/nodeCtescan.c \
../src/backend/executor/nodeCustom.c \
../src/backend/executor/nodeForeignscan.c \
../src/backend/executor/nodeFunctionscan.c \
../src/backend/executor/nodeGroup.c \
../src/backend/executor/nodeHash.c \
../src/backend/executor/nodeHashjoin.c \
../src/backend/executor/nodeIndexonlyscan.c \
../src/backend/executor/nodeIndexscan.c \
../src/backend/executor/nodeLimit.c \
../src/backend/executor/nodeLockRows.c \
../src/backend/executor/nodeMaterial.c \
../src/backend/executor/nodeMergeAppend.c \
../src/backend/executor/nodeMergejoin.c \
../src/backend/executor/nodeModifyTable.c \
../src/backend/executor/nodeNestloop.c \
../src/backend/executor/nodeRecursiveunion.c \
../src/backend/executor/nodeResult.c \
../src/backend/executor/nodeSeqscan.c \
../src/backend/executor/nodeSetOp.c \
../src/backend/executor/nodeSort.c \
../src/backend/executor/nodeSubplan.c \
../src/backend/executor/nodeSubqueryscan.c \
../src/backend/executor/nodeTidscan.c \
../src/backend/executor/nodeUnique.c \
../src/backend/executor/nodeValuesscan.c \
../src/backend/executor/nodeWindowAgg.c \
../src/backend/executor/nodeWorktablescan.c \
../src/backend/executor/spi.c \
../src/backend/executor/tstoreReceiver.c 

OBJS += \
./src/backend/executor/execAmi.o \
./src/backend/executor/execCurrent.o \
./src/backend/executor/execGrouping.o \
./src/backend/executor/execJunk.o \
./src/backend/executor/execMain.o \
./src/backend/executor/execProcnode.o \
./src/backend/executor/execQual.o \
./src/backend/executor/execScan.o \
./src/backend/executor/execTuples.o \
./src/backend/executor/execUtils.o \
./src/backend/executor/functions.o \
./src/backend/executor/instrument.o \
./src/backend/executor/nodeAgg.o \
./src/backend/executor/nodeAppend.o \
./src/backend/executor/nodeBitmapAnd.o \
./src/backend/executor/nodeBitmapHeapscan.o \
./src/backend/executor/nodeBitmapIndexscan.o \
./src/backend/executor/nodeBitmapOr.o \
./src/backend/executor/nodeCtescan.o \
./src/backend/executor/nodeCustom.o \
./src/backend/executor/nodeForeignscan.o \
./src/backend/executor/nodeFunctionscan.o \
./src/backend/executor/nodeGroup.o \
./src/backend/executor/nodeHash.o \
./src/backend/executor/nodeHashjoin.o \
./src/backend/executor/nodeIndexonlyscan.o \
./src/backend/executor/nodeIndexscan.o \
./src/backend/executor/nodeLimit.o \
./src/backend/executor/nodeLockRows.o \
./src/backend/executor/nodeMaterial.o \
./src/backend/executor/nodeMergeAppend.o \
./src/backend/executor/nodeMergejoin.o \
./src/backend/executor/nodeModifyTable.o \
./src/backend/executor/nodeNestloop.o \
./src/backend/executor/nodeRecursiveunion.o \
./src/backend/executor/nodeResult.o \
./src/backend/executor/nodeSeqscan.o \
./src/backend/executor/nodeSetOp.o \
./src/backend/executor/nodeSort.o \
./src/backend/executor/nodeSubplan.o \
./src/backend/executor/nodeSubqueryscan.o \
./src/backend/executor/nodeTidscan.o \
./src/backend/executor/nodeUnique.o \
./src/backend/executor/nodeValuesscan.o \
./src/backend/executor/nodeWindowAgg.o \
./src/backend/executor/nodeWorktablescan.o \
./src/backend/executor/spi.o \
./src/backend/executor/tstoreReceiver.o 

C_DEPS += \
./src/backend/executor/execAmi.d \
./src/backend/executor/execCurrent.d \
./src/backend/executor/execGrouping.d \
./src/backend/executor/execJunk.d \
./src/backend/executor/execMain.d \
./src/backend/executor/execProcnode.d \
./src/backend/executor/execQual.d \
./src/backend/executor/execScan.d \
./src/backend/executor/execTuples.d \
./src/backend/executor/execUtils.d \
./src/backend/executor/functions.d \
./src/backend/executor/instrument.d \
./src/backend/executor/nodeAgg.d \
./src/backend/executor/nodeAppend.d \
./src/backend/executor/nodeBitmapAnd.d \
./src/backend/executor/nodeBitmapHeapscan.d \
./src/backend/executor/nodeBitmapIndexscan.d \
./src/backend/executor/nodeBitmapOr.d \
./src/backend/executor/nodeCtescan.d \
./src/backend/executor/nodeCustom.d \
./src/backend/executor/nodeForeignscan.d \
./src/backend/executor/nodeFunctionscan.d \
./src/backend/executor/nodeGroup.d \
./src/backend/executor/nodeHash.d \
./src/backend/executor/nodeHashjoin.d \
./src/backend/executor/nodeIndexonlyscan.d \
./src/backend/executor/nodeIndexscan.d \
./src/backend/executor/nodeLimit.d \
./src/backend/executor/nodeLockRows.d \
./src/backend/executor/nodeMaterial.d \
./src/backend/executor/nodeMergeAppend.d \
./src/backend/executor/nodeMergejoin.d \
./src/backend/executor/nodeModifyTable.d \
./src/backend/executor/nodeNestloop.d \
./src/backend/executor/nodeRecursiveunion.d \
./src/backend/executor/nodeResult.d \
./src/backend/executor/nodeSeqscan.d \
./src/backend/executor/nodeSetOp.d \
./src/backend/executor/nodeSort.d \
./src/backend/executor/nodeSubplan.d \
./src/backend/executor/nodeSubqueryscan.d \
./src/backend/executor/nodeTidscan.d \
./src/backend/executor/nodeUnique.d \
./src/backend/executor/nodeValuesscan.d \
./src/backend/executor/nodeWindowAgg.d \
./src/backend/executor/nodeWorktablescan.d \
./src/backend/executor/spi.d \
./src/backend/executor/tstoreReceiver.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/executor/%.o: ../src/backend/executor/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


