################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/main/main.o 

C_SRCS += \
../src/backend/main/main.c 

OBJS += \
./src/backend/main/main.o 

C_DEPS += \
./src/backend/main/main.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/main/%.o: ../src/backend/main/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


