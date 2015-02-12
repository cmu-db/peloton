################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/vacuumlo/vacuumlo.c 

OBJS += \
./contrib/vacuumlo/vacuumlo.o 

C_DEPS += \
./contrib/vacuumlo/vacuumlo.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/vacuumlo/%.o: ../contrib/vacuumlo/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


