################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/bin/pgevent/pgevent.c 

OBJS += \
./src/bin/pgevent/pgevent.o 

C_DEPS += \
./src/bin/pgevent/pgevent.d 


# Each subdirectory must supply rules for building sources it contributes
src/bin/pgevent/%.o: ../src/bin/pgevent/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


