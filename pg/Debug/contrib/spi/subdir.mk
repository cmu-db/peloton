################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../contrib/spi/autoinc.o \
../contrib/spi/insert_username.o \
../contrib/spi/moddatetime.o \
../contrib/spi/refint.o \
../contrib/spi/timetravel.o 

C_SRCS += \
../contrib/spi/autoinc.c \
../contrib/spi/insert_username.c \
../contrib/spi/moddatetime.c \
../contrib/spi/refint.c \
../contrib/spi/timetravel.c 

OBJS += \
./contrib/spi/autoinc.o \
./contrib/spi/insert_username.o \
./contrib/spi/moddatetime.o \
./contrib/spi/refint.o \
./contrib/spi/timetravel.o 

C_DEPS += \
./contrib/spi/autoinc.d \
./contrib/spi/insert_username.d \
./contrib/spi/moddatetime.d \
./contrib/spi/refint.d \
./contrib/spi/timetravel.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/spi/%.o: ../contrib/spi/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


