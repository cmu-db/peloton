################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/interfaces/libpq/test/uri-regress.c 

OBJS += \
./src/interfaces/libpq/test/uri-regress.o 

C_DEPS += \
./src/interfaces/libpq/test/uri-regress.d 


# Each subdirectory must supply rules for building sources it contributes
src/interfaces/libpq/test/%.o: ../src/interfaces/libpq/test/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


