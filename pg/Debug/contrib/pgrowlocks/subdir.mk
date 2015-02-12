################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/pgrowlocks/pgrowlocks.c 

OBJS += \
./contrib/pgrowlocks/pgrowlocks.o 

C_DEPS += \
./contrib/pgrowlocks/pgrowlocks.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/pgrowlocks/%.o: ../contrib/pgrowlocks/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


