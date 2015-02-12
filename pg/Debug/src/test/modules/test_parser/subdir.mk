################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/test/modules/test_parser/test_parser.c 

OBJS += \
./src/test/modules/test_parser/test_parser.o 

C_DEPS += \
./src/test/modules/test_parser/test_parser.d 


# Each subdirectory must supply rules for building sources it contributes
src/test/modules/test_parser/%.o: ../src/test/modules/test_parser/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


