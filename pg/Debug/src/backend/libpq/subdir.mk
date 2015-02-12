################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/libpq/auth.o \
../src/backend/libpq/be-fsstubs.o \
../src/backend/libpq/be-secure.o \
../src/backend/libpq/crypt.o \
../src/backend/libpq/hba.o \
../src/backend/libpq/ip.o \
../src/backend/libpq/md5.o \
../src/backend/libpq/pqcomm.o \
../src/backend/libpq/pqformat.o \
../src/backend/libpq/pqmq.o \
../src/backend/libpq/pqsignal.o 

C_SRCS += \
../src/backend/libpq/auth.c \
../src/backend/libpq/be-fsstubs.c \
../src/backend/libpq/be-secure-openssl.c \
../src/backend/libpq/be-secure.c \
../src/backend/libpq/crypt.c \
../src/backend/libpq/hba.c \
../src/backend/libpq/ip.c \
../src/backend/libpq/md5.c \
../src/backend/libpq/pqcomm.c \
../src/backend/libpq/pqformat.c \
../src/backend/libpq/pqmq.c \
../src/backend/libpq/pqsignal.c 

OBJS += \
./src/backend/libpq/auth.o \
./src/backend/libpq/be-fsstubs.o \
./src/backend/libpq/be-secure-openssl.o \
./src/backend/libpq/be-secure.o \
./src/backend/libpq/crypt.o \
./src/backend/libpq/hba.o \
./src/backend/libpq/ip.o \
./src/backend/libpq/md5.o \
./src/backend/libpq/pqcomm.o \
./src/backend/libpq/pqformat.o \
./src/backend/libpq/pqmq.o \
./src/backend/libpq/pqsignal.o 

C_DEPS += \
./src/backend/libpq/auth.d \
./src/backend/libpq/be-fsstubs.d \
./src/backend/libpq/be-secure-openssl.d \
./src/backend/libpq/be-secure.d \
./src/backend/libpq/crypt.d \
./src/backend/libpq/hba.d \
./src/backend/libpq/ip.d \
./src/backend/libpq/md5.d \
./src/backend/libpq/pqcomm.d \
./src/backend/libpq/pqformat.d \
./src/backend/libpq/pqmq.d \
./src/backend/libpq/pqsignal.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/libpq/%.o: ../src/backend/libpq/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


