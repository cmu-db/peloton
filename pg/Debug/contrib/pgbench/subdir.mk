################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../contrib/pgbench/pgbench.o 

C_SRCS += \
../contrib/pgbench/pgbench.c 

OBJS += \
./contrib/pgbench/pgbench.o 

C_DEPS += \
./contrib/pgbench/pgbench.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/pgbench/%.o: ../contrib/pgbench/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


