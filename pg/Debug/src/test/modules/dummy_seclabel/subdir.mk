################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/test/modules/dummy_seclabel/dummy_seclabel.c 

OBJS += \
./src/test/modules/dummy_seclabel/dummy_seclabel.o 

C_DEPS += \
./src/test/modules/dummy_seclabel/dummy_seclabel.d 


# Each subdirectory must supply rules for building sources it contributes
src/test/modules/dummy_seclabel/%.o: ../src/test/modules/dummy_seclabel/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


