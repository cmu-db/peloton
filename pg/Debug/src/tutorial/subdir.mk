################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/tutorial/complex.c \
../src/tutorial/funcs.c \
../src/tutorial/funcs_new.c 

OBJS += \
./src/tutorial/complex.o \
./src/tutorial/funcs.o \
./src/tutorial/funcs_new.o 

C_DEPS += \
./src/tutorial/complex.d \
./src/tutorial/funcs.d \
./src/tutorial/funcs_new.d 


# Each subdirectory must supply rules for building sources it contributes
src/tutorial/%.o: ../src/tutorial/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


