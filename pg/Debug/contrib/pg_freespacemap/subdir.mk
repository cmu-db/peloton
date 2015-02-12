################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/pg_freespacemap/pg_freespacemap.c 

OBJS += \
./contrib/pg_freespacemap/pg_freespacemap.o 

C_DEPS += \
./contrib/pg_freespacemap/pg_freespacemap.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/pg_freespacemap/%.o: ../contrib/pg_freespacemap/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


