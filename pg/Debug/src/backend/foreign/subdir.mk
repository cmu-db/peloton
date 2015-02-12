################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/foreign/foreign.o 

C_SRCS += \
../src/backend/foreign/foreign.c 

OBJS += \
./src/backend/foreign/foreign.o 

C_DEPS += \
./src/backend/foreign/foreign.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/foreign/%.o: ../src/backend/foreign/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


