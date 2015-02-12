################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/unaccent/unaccent.c 

OBJS += \
./contrib/unaccent/unaccent.o 

C_DEPS += \
./contrib/unaccent/unaccent.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/unaccent/%.o: ../contrib/unaccent/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


