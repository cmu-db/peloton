################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/tools/findoidjoins/findoidjoins.c 

OBJS += \
./src/tools/findoidjoins/findoidjoins.o 

C_DEPS += \
./src/tools/findoidjoins/findoidjoins.d 


# Each subdirectory must supply rules for building sources it contributes
src/tools/findoidjoins/%.o: ../src/tools/findoidjoins/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


