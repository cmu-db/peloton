################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/bin/pg_basebackup/pg_basebackup.o \
../src/bin/pg_basebackup/pg_receivexlog.o \
../src/bin/pg_basebackup/pg_recvlogical.o \
../src/bin/pg_basebackup/receivelog.o \
../src/bin/pg_basebackup/streamutil.o 

C_SRCS += \
../src/bin/pg_basebackup/pg_basebackup.c \
../src/bin/pg_basebackup/pg_receivexlog.c \
../src/bin/pg_basebackup/pg_recvlogical.c \
../src/bin/pg_basebackup/receivelog.c \
../src/bin/pg_basebackup/streamutil.c 

OBJS += \
./src/bin/pg_basebackup/pg_basebackup.o \
./src/bin/pg_basebackup/pg_receivexlog.o \
./src/bin/pg_basebackup/pg_recvlogical.o \
./src/bin/pg_basebackup/receivelog.o \
./src/bin/pg_basebackup/streamutil.o 

C_DEPS += \
./src/bin/pg_basebackup/pg_basebackup.d \
./src/bin/pg_basebackup/pg_receivexlog.d \
./src/bin/pg_basebackup/pg_recvlogical.d \
./src/bin/pg_basebackup/receivelog.d \
./src/bin/pg_basebackup/streamutil.d 


# Each subdirectory must supply rules for building sources it contributes
src/bin/pg_basebackup/%.o: ../src/bin/pg_basebackup/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


