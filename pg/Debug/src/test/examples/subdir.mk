################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/test/examples/testlibpq.c \
../src/test/examples/testlibpq2.c \
../src/test/examples/testlibpq3.c \
../src/test/examples/testlibpq4.c \
../src/test/examples/testlo.c \
../src/test/examples/testlo64.c 

OBJS += \
./src/test/examples/testlibpq.o \
./src/test/examples/testlibpq2.o \
./src/test/examples/testlibpq3.o \
./src/test/examples/testlibpq4.o \
./src/test/examples/testlo.o \
./src/test/examples/testlo64.o 

C_DEPS += \
./src/test/examples/testlibpq.d \
./src/test/examples/testlibpq2.d \
./src/test/examples/testlibpq3.d \
./src/test/examples/testlibpq4.d \
./src/test/examples/testlo.d \
./src/test/examples/testlo64.d 


# Each subdirectory must supply rules for building sources it contributes
src/test/examples/%.o: ../src/test/examples/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


