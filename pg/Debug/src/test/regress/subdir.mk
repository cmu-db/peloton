################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/test/regress/pg_regress.o \
../src/test/regress/pg_regress_main.o \
../src/test/regress/regress.o 

C_SRCS += \
../src/test/regress/pg_regress.c \
../src/test/regress/pg_regress_main.c \
../src/test/regress/regress.c 

OBJS += \
./src/test/regress/pg_regress.o \
./src/test/regress/pg_regress_main.o \
./src/test/regress/regress.o 

C_DEPS += \
./src/test/regress/pg_regress.d \
./src/test/regress/pg_regress_main.d \
./src/test/regress/regress.d 


# Each subdirectory must supply rules for building sources it contributes
src/test/regress/%.o: ../src/test/regress/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


