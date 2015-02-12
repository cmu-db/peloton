################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/xml2/xpath.c \
../contrib/xml2/xslt_proc.c 

OBJS += \
./contrib/xml2/xpath.o \
./contrib/xml2/xslt_proc.o 

C_DEPS += \
./contrib/xml2/xpath.d \
./contrib/xml2/xslt_proc.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/xml2/%.o: ../contrib/xml2/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


