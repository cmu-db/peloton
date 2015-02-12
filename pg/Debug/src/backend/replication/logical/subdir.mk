################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/replication/logical/decode.o \
../src/backend/replication/logical/logical.o \
../src/backend/replication/logical/logicalfuncs.o \
../src/backend/replication/logical/reorderbuffer.o \
../src/backend/replication/logical/snapbuild.o 

C_SRCS += \
../src/backend/replication/logical/decode.c \
../src/backend/replication/logical/logical.c \
../src/backend/replication/logical/logicalfuncs.c \
../src/backend/replication/logical/reorderbuffer.c \
../src/backend/replication/logical/snapbuild.c 

OBJS += \
./src/backend/replication/logical/decode.o \
./src/backend/replication/logical/logical.o \
./src/backend/replication/logical/logicalfuncs.o \
./src/backend/replication/logical/reorderbuffer.o \
./src/backend/replication/logical/snapbuild.o 

C_DEPS += \
./src/backend/replication/logical/decode.d \
./src/backend/replication/logical/logical.d \
./src/backend/replication/logical/logicalfuncs.d \
./src/backend/replication/logical/reorderbuffer.d \
./src/backend/replication/logical/snapbuild.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/replication/logical/%.o: ../src/backend/replication/logical/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


