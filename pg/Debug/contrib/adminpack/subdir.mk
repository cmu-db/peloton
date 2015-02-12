################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/adminpack/adminpack.c 

OBJS += \
./contrib/adminpack/adminpack.o 

C_DEPS += \
./contrib/adminpack/adminpack.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/adminpack/%.o: ../contrib/adminpack/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


