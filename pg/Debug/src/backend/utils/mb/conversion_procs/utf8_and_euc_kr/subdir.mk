################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/utils/mb/conversion_procs/utf8_and_euc_kr/utf8_and_euc_kr.o 

C_SRCS += \
../src/backend/utils/mb/conversion_procs/utf8_and_euc_kr/utf8_and_euc_kr.c 

OBJS += \
./src/backend/utils/mb/conversion_procs/utf8_and_euc_kr/utf8_and_euc_kr.o 

C_DEPS += \
./src/backend/utils/mb/conversion_procs/utf8_and_euc_kr/utf8_and_euc_kr.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/utils/mb/conversion_procs/utf8_and_euc_kr/%.o: ../src/backend/utils/mb/conversion_procs/utf8_and_euc_kr/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


