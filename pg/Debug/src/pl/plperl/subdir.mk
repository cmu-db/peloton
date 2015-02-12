################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/pl/plperl/plperl.c 

OBJS += \
./src/pl/plperl/plperl.o 

C_DEPS += \
./src/pl/plperl/plperl.d 


# Each subdirectory must supply rules for building sources it contributes
src/pl/plperl/%.o: ../src/pl/plperl/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


