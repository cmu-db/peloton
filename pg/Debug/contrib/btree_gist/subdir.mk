################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/btree_gist/btree_bit.c \
../contrib/btree_gist/btree_bytea.c \
../contrib/btree_gist/btree_cash.c \
../contrib/btree_gist/btree_date.c \
../contrib/btree_gist/btree_float4.c \
../contrib/btree_gist/btree_float8.c \
../contrib/btree_gist/btree_gist.c \
../contrib/btree_gist/btree_inet.c \
../contrib/btree_gist/btree_int2.c \
../contrib/btree_gist/btree_int4.c \
../contrib/btree_gist/btree_int8.c \
../contrib/btree_gist/btree_interval.c \
../contrib/btree_gist/btree_macaddr.c \
../contrib/btree_gist/btree_numeric.c \
../contrib/btree_gist/btree_oid.c \
../contrib/btree_gist/btree_text.c \
../contrib/btree_gist/btree_time.c \
../contrib/btree_gist/btree_ts.c \
../contrib/btree_gist/btree_utils_num.c \
../contrib/btree_gist/btree_utils_var.c 

OBJS += \
./contrib/btree_gist/btree_bit.o \
./contrib/btree_gist/btree_bytea.o \
./contrib/btree_gist/btree_cash.o \
./contrib/btree_gist/btree_date.o \
./contrib/btree_gist/btree_float4.o \
./contrib/btree_gist/btree_float8.o \
./contrib/btree_gist/btree_gist.o \
./contrib/btree_gist/btree_inet.o \
./contrib/btree_gist/btree_int2.o \
./contrib/btree_gist/btree_int4.o \
./contrib/btree_gist/btree_int8.o \
./contrib/btree_gist/btree_interval.o \
./contrib/btree_gist/btree_macaddr.o \
./contrib/btree_gist/btree_numeric.o \
./contrib/btree_gist/btree_oid.o \
./contrib/btree_gist/btree_text.o \
./contrib/btree_gist/btree_time.o \
./contrib/btree_gist/btree_ts.o \
./contrib/btree_gist/btree_utils_num.o \
./contrib/btree_gist/btree_utils_var.o 

C_DEPS += \
./contrib/btree_gist/btree_bit.d \
./contrib/btree_gist/btree_bytea.d \
./contrib/btree_gist/btree_cash.d \
./contrib/btree_gist/btree_date.d \
./contrib/btree_gist/btree_float4.d \
./contrib/btree_gist/btree_float8.d \
./contrib/btree_gist/btree_gist.d \
./contrib/btree_gist/btree_inet.d \
./contrib/btree_gist/btree_int2.d \
./contrib/btree_gist/btree_int4.d \
./contrib/btree_gist/btree_int8.d \
./contrib/btree_gist/btree_interval.d \
./contrib/btree_gist/btree_macaddr.d \
./contrib/btree_gist/btree_numeric.d \
./contrib/btree_gist/btree_oid.d \
./contrib/btree_gist/btree_text.d \
./contrib/btree_gist/btree_time.d \
./contrib/btree_gist/btree_ts.d \
./contrib/btree_gist/btree_utils_num.d \
./contrib/btree_gist/btree_utils_var.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/btree_gist/%.o: ../contrib/btree_gist/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


