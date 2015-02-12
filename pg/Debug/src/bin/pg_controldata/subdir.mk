################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/bin/pg_controldata/pg_controldata.o 

C_SRCS += \
../src/bin/pg_controldata/pg_controldata.c 

OBJS += \
./src/bin/pg_controldata/pg_controldata.o 

C_DEPS += \
./src/bin/pg_controldata/pg_controldata.d 


# Each subdirectory must supply rules for building sources it contributes
src/bin/pg_controldata/%.o: ../src/bin/pg_controldata/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


