################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.o 

C_SRCS += \
../src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c 

OBJS += \
./src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.o 

C_DEPS += \
./src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/utils/mb/conversion_procs/latin2_and_win1250/%.o: ../src/backend/utils/mb/conversion_procs/latin2_and_win1250/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


