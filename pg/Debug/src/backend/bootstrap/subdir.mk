################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/bootstrap/bootparse.o \
../src/backend/bootstrap/bootstrap.o 

C_SRCS += \
../src/backend/bootstrap/bootparse.c \
../src/backend/bootstrap/bootscanner.c \
../src/backend/bootstrap/bootstrap.c 

OBJS += \
./src/backend/bootstrap/bootparse.o \
./src/backend/bootstrap/bootscanner.o \
./src/backend/bootstrap/bootstrap.o 

C_DEPS += \
./src/backend/bootstrap/bootparse.d \
./src/backend/bootstrap/bootscanner.d \
./src/backend/bootstrap/bootstrap.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/bootstrap/%.o: ../src/backend/bootstrap/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


