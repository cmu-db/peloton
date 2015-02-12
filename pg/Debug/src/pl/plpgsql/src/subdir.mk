################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/pl/plpgsql/src/pl_comp.o \
../src/pl/plpgsql/src/pl_exec.o \
../src/pl/plpgsql/src/pl_funcs.o \
../src/pl/plpgsql/src/pl_gram.o \
../src/pl/plpgsql/src/pl_handler.o \
../src/pl/plpgsql/src/pl_scanner.o 

C_SRCS += \
../src/pl/plpgsql/src/pl_comp.c \
../src/pl/plpgsql/src/pl_exec.c \
../src/pl/plpgsql/src/pl_funcs.c \
../src/pl/plpgsql/src/pl_gram.c \
../src/pl/plpgsql/src/pl_handler.c \
../src/pl/plpgsql/src/pl_scanner.c 

OBJS += \
./src/pl/plpgsql/src/pl_comp.o \
./src/pl/plpgsql/src/pl_exec.o \
./src/pl/plpgsql/src/pl_funcs.o \
./src/pl/plpgsql/src/pl_gram.o \
./src/pl/plpgsql/src/pl_handler.o \
./src/pl/plpgsql/src/pl_scanner.o 

C_DEPS += \
./src/pl/plpgsql/src/pl_comp.d \
./src/pl/plpgsql/src/pl_exec.d \
./src/pl/plpgsql/src/pl_funcs.d \
./src/pl/plpgsql/src/pl_gram.d \
./src/pl/plpgsql/src/pl_handler.d \
./src/pl/plpgsql/src/pl_scanner.d 


# Each subdirectory must supply rules for building sources it contributes
src/pl/plpgsql/src/%.o: ../src/pl/plpgsql/src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


