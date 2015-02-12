################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/oid2name/oid2name.c 

OBJS += \
./contrib/oid2name/oid2name.o 

C_DEPS += \
./contrib/oid2name/oid2name.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/oid2name/%.o: ../contrib/oid2name/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


