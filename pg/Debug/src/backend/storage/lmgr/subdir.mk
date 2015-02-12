################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/storage/lmgr/deadlock.o \
../src/backend/storage/lmgr/lmgr.o \
../src/backend/storage/lmgr/lock.o \
../src/backend/storage/lmgr/lwlock.o \
../src/backend/storage/lmgr/predicate.o \
../src/backend/storage/lmgr/proc.o \
../src/backend/storage/lmgr/s_lock.o \
../src/backend/storage/lmgr/spin.o 

C_SRCS += \
../src/backend/storage/lmgr/deadlock.c \
../src/backend/storage/lmgr/lmgr.c \
../src/backend/storage/lmgr/lock.c \
../src/backend/storage/lmgr/lwlock.c \
../src/backend/storage/lmgr/predicate.c \
../src/backend/storage/lmgr/proc.c \
../src/backend/storage/lmgr/s_lock.c \
../src/backend/storage/lmgr/spin.c 

OBJS += \
./src/backend/storage/lmgr/deadlock.o \
./src/backend/storage/lmgr/lmgr.o \
./src/backend/storage/lmgr/lock.o \
./src/backend/storage/lmgr/lwlock.o \
./src/backend/storage/lmgr/predicate.o \
./src/backend/storage/lmgr/proc.o \
./src/backend/storage/lmgr/s_lock.o \
./src/backend/storage/lmgr/spin.o 

C_DEPS += \
./src/backend/storage/lmgr/deadlock.d \
./src/backend/storage/lmgr/lmgr.d \
./src/backend/storage/lmgr/lock.d \
./src/backend/storage/lmgr/lwlock.d \
./src/backend/storage/lmgr/predicate.d \
./src/backend/storage/lmgr/proc.d \
./src/backend/storage/lmgr/s_lock.d \
./src/backend/storage/lmgr/spin.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/storage/lmgr/%.o: ../src/backend/storage/lmgr/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


