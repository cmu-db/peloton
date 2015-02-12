################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/dict_int/dict_int.c 

OBJS += \
./contrib/dict_int/dict_int.o 

C_DEPS += \
./contrib/dict_int/dict_int.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/dict_int/%.o: ../contrib/dict_int/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


