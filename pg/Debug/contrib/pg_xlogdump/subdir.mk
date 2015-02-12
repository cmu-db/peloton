################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/pg_xlogdump/compat.c \
../contrib/pg_xlogdump/pg_xlogdump.c \
../contrib/pg_xlogdump/rmgrdesc.c 

OBJS += \
./contrib/pg_xlogdump/compat.o \
./contrib/pg_xlogdump/pg_xlogdump.o \
./contrib/pg_xlogdump/rmgrdesc.o 

C_DEPS += \
./contrib/pg_xlogdump/compat.d \
./contrib/pg_xlogdump/pg_xlogdump.d \
./contrib/pg_xlogdump/rmgrdesc.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/pg_xlogdump/%.o: ../contrib/pg_xlogdump/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


