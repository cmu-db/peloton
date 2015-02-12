################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/timezone/ialloc.o \
../src/timezone/localtime.o \
../src/timezone/pgtz.o \
../src/timezone/scheck.o \
../src/timezone/strftime.o \
../src/timezone/zic.o 

C_SRCS += \
../src/timezone/ialloc.c \
../src/timezone/localtime.c \
../src/timezone/pgtz.c \
../src/timezone/scheck.c \
../src/timezone/strftime.c \
../src/timezone/zic.c 

OBJS += \
./src/timezone/ialloc.o \
./src/timezone/localtime.o \
./src/timezone/pgtz.o \
./src/timezone/scheck.o \
./src/timezone/strftime.o \
./src/timezone/zic.o 

C_DEPS += \
./src/timezone/ialloc.d \
./src/timezone/localtime.d \
./src/timezone/pgtz.d \
./src/timezone/scheck.d \
./src/timezone/strftime.d \
./src/timezone/zic.d 


# Each subdirectory must supply rules for building sources it contributes
src/timezone/%.o: ../src/timezone/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


