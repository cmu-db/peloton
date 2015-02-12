################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/storage/buffer/buf_init.o \
../src/backend/storage/buffer/buf_table.o \
../src/backend/storage/buffer/bufmgr.o \
../src/backend/storage/buffer/freelist.o \
../src/backend/storage/buffer/localbuf.o 

C_SRCS += \
../src/backend/storage/buffer/buf_init.c \
../src/backend/storage/buffer/buf_table.c \
../src/backend/storage/buffer/bufmgr.c \
../src/backend/storage/buffer/freelist.c \
../src/backend/storage/buffer/localbuf.c 

OBJS += \
./src/backend/storage/buffer/buf_init.o \
./src/backend/storage/buffer/buf_table.o \
./src/backend/storage/buffer/bufmgr.o \
./src/backend/storage/buffer/freelist.o \
./src/backend/storage/buffer/localbuf.o 

C_DEPS += \
./src/backend/storage/buffer/buf_init.d \
./src/backend/storage/buffer/buf_table.d \
./src/backend/storage/buffer/bufmgr.d \
./src/backend/storage/buffer/freelist.d \
./src/backend/storage/buffer/localbuf.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/storage/buffer/%.o: ../src/backend/storage/buffer/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


