################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/tcn/tcn.c 

OBJS += \
./contrib/tcn/tcn.o 

C_DEPS += \
./contrib/tcn/tcn.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/tcn/%.o: ../contrib/tcn/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


