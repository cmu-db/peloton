################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/test/thread/thread_test.c 

OBJS += \
./src/test/thread/thread_test.o 

C_DEPS += \
./src/test/thread/thread_test.d 


# Each subdirectory must supply rules for building sources it contributes
src/test/thread/%.o: ../src/test/thread/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


