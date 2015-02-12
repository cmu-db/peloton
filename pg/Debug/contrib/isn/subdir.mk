################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/isn/isn.c 

OBJS += \
./contrib/isn/isn.o 

C_DEPS += \
./contrib/isn/isn.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/isn/%.o: ../contrib/isn/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


