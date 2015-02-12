################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/interfaces/ecpg/pgtypeslib/common.o \
../src/interfaces/ecpg/pgtypeslib/datetime.o \
../src/interfaces/ecpg/pgtypeslib/dt_common.o \
../src/interfaces/ecpg/pgtypeslib/interval.o \
../src/interfaces/ecpg/pgtypeslib/numeric.o \
../src/interfaces/ecpg/pgtypeslib/pgstrcasecmp.o \
../src/interfaces/ecpg/pgtypeslib/timestamp.o 

C_SRCS += \
../src/interfaces/ecpg/pgtypeslib/common.c \
../src/interfaces/ecpg/pgtypeslib/datetime.c \
../src/interfaces/ecpg/pgtypeslib/dt_common.c \
../src/interfaces/ecpg/pgtypeslib/interval.c \
../src/interfaces/ecpg/pgtypeslib/numeric.c \
../src/interfaces/ecpg/pgtypeslib/pgstrcasecmp.c \
../src/interfaces/ecpg/pgtypeslib/timestamp.c 

OBJS += \
./src/interfaces/ecpg/pgtypeslib/common.o \
./src/interfaces/ecpg/pgtypeslib/datetime.o \
./src/interfaces/ecpg/pgtypeslib/dt_common.o \
./src/interfaces/ecpg/pgtypeslib/interval.o \
./src/interfaces/ecpg/pgtypeslib/numeric.o \
./src/interfaces/ecpg/pgtypeslib/pgstrcasecmp.o \
./src/interfaces/ecpg/pgtypeslib/timestamp.o 

C_DEPS += \
./src/interfaces/ecpg/pgtypeslib/common.d \
./src/interfaces/ecpg/pgtypeslib/datetime.d \
./src/interfaces/ecpg/pgtypeslib/dt_common.d \
./src/interfaces/ecpg/pgtypeslib/interval.d \
./src/interfaces/ecpg/pgtypeslib/numeric.d \
./src/interfaces/ecpg/pgtypeslib/pgstrcasecmp.d \
./src/interfaces/ecpg/pgtypeslib/timestamp.d 


# Each subdirectory must supply rules for building sources it contributes
src/interfaces/ecpg/pgtypeslib/%.o: ../src/interfaces/ecpg/pgtypeslib/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


