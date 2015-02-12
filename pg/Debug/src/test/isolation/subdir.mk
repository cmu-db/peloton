################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/test/isolation/isolation_main.c \
../src/test/isolation/isolationtester.c \
../src/test/isolation/specparse.c \
../src/test/isolation/specscanner.c 

OBJS += \
./src/test/isolation/isolation_main.o \
./src/test/isolation/isolationtester.o \
./src/test/isolation/specparse.o \
./src/test/isolation/specscanner.o 

C_DEPS += \
./src/test/isolation/isolation_main.d \
./src/test/isolation/isolationtester.d \
./src/test/isolation/specparse.d \
./src/test/isolation/specscanner.d 


# Each subdirectory must supply rules for building sources it contributes
src/test/isolation/%.o: ../src/test/isolation/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


