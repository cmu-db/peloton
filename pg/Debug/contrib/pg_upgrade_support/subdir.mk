################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/pg_upgrade_support/pg_upgrade_support.c 

OBJS += \
./contrib/pg_upgrade_support/pg_upgrade_support.o 

C_DEPS += \
./contrib/pg_upgrade_support/pg_upgrade_support.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/pg_upgrade_support/%.o: ../contrib/pg_upgrade_support/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


