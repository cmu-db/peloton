################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/test/modules/worker_spi/worker_spi.c 

OBJS += \
./src/test/modules/worker_spi/worker_spi.o 

C_DEPS += \
./src/test/modules/worker_spi/worker_spi.d 


# Each subdirectory must supply rules for building sources it contributes
src/test/modules/worker_spi/%.o: ../src/test/modules/worker_spi/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


