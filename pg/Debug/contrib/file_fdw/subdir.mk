################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/file_fdw/file_fdw.c 

OBJS += \
./contrib/file_fdw/file_fdw.o 

C_DEPS += \
./contrib/file_fdw/file_fdw.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/file_fdw/%.o: ../contrib/file_fdw/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


