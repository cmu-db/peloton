################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/pl/plpython/plpy_cursorobject.c \
../src/pl/plpython/plpy_elog.c \
../src/pl/plpython/plpy_exec.c \
../src/pl/plpython/plpy_main.c \
../src/pl/plpython/plpy_planobject.c \
../src/pl/plpython/plpy_plpymodule.c \
../src/pl/plpython/plpy_procedure.c \
../src/pl/plpython/plpy_resultobject.c \
../src/pl/plpython/plpy_spi.c \
../src/pl/plpython/plpy_subxactobject.c \
../src/pl/plpython/plpy_typeio.c \
../src/pl/plpython/plpy_util.c 

OBJS += \
./src/pl/plpython/plpy_cursorobject.o \
./src/pl/plpython/plpy_elog.o \
./src/pl/plpython/plpy_exec.o \
./src/pl/plpython/plpy_main.o \
./src/pl/plpython/plpy_planobject.o \
./src/pl/plpython/plpy_plpymodule.o \
./src/pl/plpython/plpy_procedure.o \
./src/pl/plpython/plpy_resultobject.o \
./src/pl/plpython/plpy_spi.o \
./src/pl/plpython/plpy_subxactobject.o \
./src/pl/plpython/plpy_typeio.o \
./src/pl/plpython/plpy_util.o 

C_DEPS += \
./src/pl/plpython/plpy_cursorobject.d \
./src/pl/plpython/plpy_elog.d \
./src/pl/plpython/plpy_exec.d \
./src/pl/plpython/plpy_main.d \
./src/pl/plpython/plpy_planobject.d \
./src/pl/plpython/plpy_plpymodule.d \
./src/pl/plpython/plpy_procedure.d \
./src/pl/plpython/plpy_resultobject.d \
./src/pl/plpython/plpy_spi.d \
./src/pl/plpython/plpy_subxactobject.d \
./src/pl/plpython/plpy_typeio.d \
./src/pl/plpython/plpy_util.d 


# Each subdirectory must supply rules for building sources it contributes
src/pl/plpython/%.o: ../src/pl/plpython/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


