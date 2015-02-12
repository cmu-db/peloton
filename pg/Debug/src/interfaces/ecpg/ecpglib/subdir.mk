################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/interfaces/ecpg/ecpglib/connect.o \
../src/interfaces/ecpg/ecpglib/data.o \
../src/interfaces/ecpg/ecpglib/descriptor.o \
../src/interfaces/ecpg/ecpglib/error.o \
../src/interfaces/ecpg/ecpglib/execute.o \
../src/interfaces/ecpg/ecpglib/memory.o \
../src/interfaces/ecpg/ecpglib/misc.o \
../src/interfaces/ecpg/ecpglib/path.o \
../src/interfaces/ecpg/ecpglib/pgstrcasecmp.o \
../src/interfaces/ecpg/ecpglib/prepare.o \
../src/interfaces/ecpg/ecpglib/sqlda.o \
../src/interfaces/ecpg/ecpglib/strlcpy.o \
../src/interfaces/ecpg/ecpglib/thread.o \
../src/interfaces/ecpg/ecpglib/typename.o 

C_SRCS += \
../src/interfaces/ecpg/ecpglib/connect.c \
../src/interfaces/ecpg/ecpglib/data.c \
../src/interfaces/ecpg/ecpglib/descriptor.c \
../src/interfaces/ecpg/ecpglib/error.c \
../src/interfaces/ecpg/ecpglib/execute.c \
../src/interfaces/ecpg/ecpglib/memory.c \
../src/interfaces/ecpg/ecpglib/misc.c \
../src/interfaces/ecpg/ecpglib/path.c \
../src/interfaces/ecpg/ecpglib/pgstrcasecmp.c \
../src/interfaces/ecpg/ecpglib/prepare.c \
../src/interfaces/ecpg/ecpglib/sqlda.c \
../src/interfaces/ecpg/ecpglib/strlcpy.c \
../src/interfaces/ecpg/ecpglib/thread.c \
../src/interfaces/ecpg/ecpglib/typename.c 

OBJS += \
./src/interfaces/ecpg/ecpglib/connect.o \
./src/interfaces/ecpg/ecpglib/data.o \
./src/interfaces/ecpg/ecpglib/descriptor.o \
./src/interfaces/ecpg/ecpglib/error.o \
./src/interfaces/ecpg/ecpglib/execute.o \
./src/interfaces/ecpg/ecpglib/memory.o \
./src/interfaces/ecpg/ecpglib/misc.o \
./src/interfaces/ecpg/ecpglib/path.o \
./src/interfaces/ecpg/ecpglib/pgstrcasecmp.o \
./src/interfaces/ecpg/ecpglib/prepare.o \
./src/interfaces/ecpg/ecpglib/sqlda.o \
./src/interfaces/ecpg/ecpglib/strlcpy.o \
./src/interfaces/ecpg/ecpglib/thread.o \
./src/interfaces/ecpg/ecpglib/typename.o 

C_DEPS += \
./src/interfaces/ecpg/ecpglib/connect.d \
./src/interfaces/ecpg/ecpglib/data.d \
./src/interfaces/ecpg/ecpglib/descriptor.d \
./src/interfaces/ecpg/ecpglib/error.d \
./src/interfaces/ecpg/ecpglib/execute.d \
./src/interfaces/ecpg/ecpglib/memory.d \
./src/interfaces/ecpg/ecpglib/misc.d \
./src/interfaces/ecpg/ecpglib/path.d \
./src/interfaces/ecpg/ecpglib/pgstrcasecmp.d \
./src/interfaces/ecpg/ecpglib/prepare.d \
./src/interfaces/ecpg/ecpglib/sqlda.d \
./src/interfaces/ecpg/ecpglib/strlcpy.d \
./src/interfaces/ecpg/ecpglib/thread.d \
./src/interfaces/ecpg/ecpglib/typename.d 


# Each subdirectory must supply rules for building sources it contributes
src/interfaces/ecpg/ecpglib/%.o: ../src/interfaces/ecpg/ecpglib/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


