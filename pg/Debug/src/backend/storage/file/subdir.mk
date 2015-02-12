################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/storage/file/buffile.o \
../src/backend/storage/file/copydir.o \
../src/backend/storage/file/fd.o \
../src/backend/storage/file/reinit.o 

C_SRCS += \
../src/backend/storage/file/buffile.c \
../src/backend/storage/file/copydir.c \
../src/backend/storage/file/fd.c \
../src/backend/storage/file/reinit.c 

OBJS += \
./src/backend/storage/file/buffile.o \
./src/backend/storage/file/copydir.o \
./src/backend/storage/file/fd.o \
./src/backend/storage/file/reinit.o 

C_DEPS += \
./src/backend/storage/file/buffile.d \
./src/backend/storage/file/copydir.d \
./src/backend/storage/file/fd.d \
./src/backend/storage/file/reinit.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/storage/file/%.o: ../src/backend/storage/file/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


