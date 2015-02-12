################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/interfaces/ecpg/compatlib/informix.o 

C_SRCS += \
../src/interfaces/ecpg/compatlib/informix.c 

OBJS += \
./src/interfaces/ecpg/compatlib/informix.o 

C_DEPS += \
./src/interfaces/ecpg/compatlib/informix.d 


# Each subdirectory must supply rules for building sources it contributes
src/interfaces/ecpg/compatlib/%.o: ../src/interfaces/ecpg/compatlib/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


