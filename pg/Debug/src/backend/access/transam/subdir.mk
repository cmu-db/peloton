################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/access/transam/clog.o \
../src/backend/access/transam/commit_ts.o \
../src/backend/access/transam/multixact.o \
../src/backend/access/transam/rmgr.o \
../src/backend/access/transam/slru.o \
../src/backend/access/transam/subtrans.o \
../src/backend/access/transam/timeline.o \
../src/backend/access/transam/transam.o \
../src/backend/access/transam/twophase.o \
../src/backend/access/transam/twophase_rmgr.o \
../src/backend/access/transam/varsup.o \
../src/backend/access/transam/xact.o \
../src/backend/access/transam/xlog.o \
../src/backend/access/transam/xlogarchive.o \
../src/backend/access/transam/xlogfuncs.o \
../src/backend/access/transam/xloginsert.o \
../src/backend/access/transam/xlogreader.o \
../src/backend/access/transam/xlogutils.o 

C_SRCS += \
../src/backend/access/transam/clog.c \
../src/backend/access/transam/commit_ts.c \
../src/backend/access/transam/multixact.c \
../src/backend/access/transam/rmgr.c \
../src/backend/access/transam/slru.c \
../src/backend/access/transam/subtrans.c \
../src/backend/access/transam/timeline.c \
../src/backend/access/transam/transam.c \
../src/backend/access/transam/twophase.c \
../src/backend/access/transam/twophase_rmgr.c \
../src/backend/access/transam/varsup.c \
../src/backend/access/transam/xact.c \
../src/backend/access/transam/xlog.c \
../src/backend/access/transam/xlogarchive.c \
../src/backend/access/transam/xlogfuncs.c \
../src/backend/access/transam/xloginsert.c \
../src/backend/access/transam/xlogreader.c \
../src/backend/access/transam/xlogutils.c 

OBJS += \
./src/backend/access/transam/clog.o \
./src/backend/access/transam/commit_ts.o \
./src/backend/access/transam/multixact.o \
./src/backend/access/transam/rmgr.o \
./src/backend/access/transam/slru.o \
./src/backend/access/transam/subtrans.o \
./src/backend/access/transam/timeline.o \
./src/backend/access/transam/transam.o \
./src/backend/access/transam/twophase.o \
./src/backend/access/transam/twophase_rmgr.o \
./src/backend/access/transam/varsup.o \
./src/backend/access/transam/xact.o \
./src/backend/access/transam/xlog.o \
./src/backend/access/transam/xlogarchive.o \
./src/backend/access/transam/xlogfuncs.o \
./src/backend/access/transam/xloginsert.o \
./src/backend/access/transam/xlogreader.o \
./src/backend/access/transam/xlogutils.o 

C_DEPS += \
./src/backend/access/transam/clog.d \
./src/backend/access/transam/commit_ts.d \
./src/backend/access/transam/multixact.d \
./src/backend/access/transam/rmgr.d \
./src/backend/access/transam/slru.d \
./src/backend/access/transam/subtrans.d \
./src/backend/access/transam/timeline.d \
./src/backend/access/transam/transam.d \
./src/backend/access/transam/twophase.d \
./src/backend/access/transam/twophase_rmgr.d \
./src/backend/access/transam/varsup.d \
./src/backend/access/transam/xact.d \
./src/backend/access/transam/xlog.d \
./src/backend/access/transam/xlogarchive.d \
./src/backend/access/transam/xlogfuncs.d \
./src/backend/access/transam/xloginsert.d \
./src/backend/access/transam/xlogreader.d \
./src/backend/access/transam/xlogutils.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/access/transam/%.o: ../src/backend/access/transam/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


