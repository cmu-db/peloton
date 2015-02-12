################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/postgres_fdw/connection.c \
../contrib/postgres_fdw/deparse.c \
../contrib/postgres_fdw/option.c \
../contrib/postgres_fdw/postgres_fdw.c 

OBJS += \
./contrib/postgres_fdw/connection.o \
./contrib/postgres_fdw/deparse.o \
./contrib/postgres_fdw/option.o \
./contrib/postgres_fdw/postgres_fdw.o 

C_DEPS += \
./contrib/postgres_fdw/connection.d \
./contrib/postgres_fdw/deparse.d \
./contrib/postgres_fdw/option.d \
./contrib/postgres_fdw/postgres_fdw.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/postgres_fdw/%.o: ../contrib/postgres_fdw/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


