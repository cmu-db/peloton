################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/pg_standby/pg_standby.c 

OBJS += \
./contrib/pg_standby/pg_standby.o 

C_DEPS += \
./contrib/pg_standby/pg_standby.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/pg_standby/%.o: ../contrib/pg_standby/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


