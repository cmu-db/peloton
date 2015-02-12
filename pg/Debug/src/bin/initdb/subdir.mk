################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/bin/initdb/encnames.o \
../src/bin/initdb/findtimezone.o \
../src/bin/initdb/initdb.o \
../src/bin/initdb/localtime.o 

C_SRCS += \
../src/bin/initdb/encnames.c \
../src/bin/initdb/findtimezone.c \
../src/bin/initdb/initdb.c \
../src/bin/initdb/localtime.c 

OBJS += \
./src/bin/initdb/encnames.o \
./src/bin/initdb/findtimezone.o \
./src/bin/initdb/initdb.o \
./src/bin/initdb/localtime.o 

C_DEPS += \
./src/bin/initdb/encnames.d \
./src/bin/initdb/findtimezone.d \
./src/bin/initdb/initdb.d \
./src/bin/initdb/localtime.d 


# Each subdirectory must supply rules for building sources it contributes
src/bin/initdb/%.o: ../src/bin/initdb/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


