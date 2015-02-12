################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/utils/mb/conversion_procs/utf8_and_gbk/utf8_and_gbk.o 

C_SRCS += \
../src/backend/utils/mb/conversion_procs/utf8_and_gbk/utf8_and_gbk.c 

OBJS += \
./src/backend/utils/mb/conversion_procs/utf8_and_gbk/utf8_and_gbk.o 

C_DEPS += \
./src/backend/utils/mb/conversion_procs/utf8_and_gbk/utf8_and_gbk.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/utils/mb/conversion_procs/utf8_and_gbk/%.o: ../src/backend/utils/mb/conversion_procs/utf8_and_gbk/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


