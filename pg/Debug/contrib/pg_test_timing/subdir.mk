################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/pg_test_timing/pg_test_timing.c 

OBJS += \
./contrib/pg_test_timing/pg_test_timing.o 

C_DEPS += \
./contrib/pg_test_timing/pg_test_timing.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/pg_test_timing/%.o: ../contrib/pg_test_timing/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


