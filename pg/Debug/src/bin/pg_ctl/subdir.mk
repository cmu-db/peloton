################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/bin/pg_ctl/pg_ctl.o 

C_SRCS += \
../src/bin/pg_ctl/pg_ctl.c 

OBJS += \
./src/bin/pg_ctl/pg_ctl.o 

C_DEPS += \
./src/bin/pg_ctl/pg_ctl.d 


# Each subdirectory must supply rules for building sources it contributes
src/bin/pg_ctl/%.o: ../src/bin/pg_ctl/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


