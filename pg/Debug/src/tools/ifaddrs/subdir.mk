################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/tools/ifaddrs/test_ifaddrs.c 

OBJS += \
./src/tools/ifaddrs/test_ifaddrs.o 

C_DEPS += \
./src/tools/ifaddrs/test_ifaddrs.d 


# Each subdirectory must supply rules for building sources it contributes
src/tools/ifaddrs/%.o: ../src/tools/ifaddrs/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


