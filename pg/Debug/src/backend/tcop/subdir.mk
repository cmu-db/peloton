################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/tcop/dest.o \
../src/backend/tcop/fastpath.o \
../src/backend/tcop/postgres.o \
../src/backend/tcop/pquery.o \
../src/backend/tcop/utility.o 

C_SRCS += \
../src/backend/tcop/dest.c \
../src/backend/tcop/fastpath.c \
../src/backend/tcop/postgres.c \
../src/backend/tcop/pquery.c \
../src/backend/tcop/utility.c 

OBJS += \
./src/backend/tcop/dest.o \
./src/backend/tcop/fastpath.o \
./src/backend/tcop/postgres.o \
./src/backend/tcop/pquery.o \
./src/backend/tcop/utility.o 

C_DEPS += \
./src/backend/tcop/dest.d \
./src/backend/tcop/fastpath.d \
./src/backend/tcop/postgres.d \
./src/backend/tcop/pquery.d \
./src/backend/tcop/utility.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/tcop/%.o: ../src/backend/tcop/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


