################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/common/exec.o \
../src/common/exec_srv.o \
../src/common/fe_memutils.o \
../src/common/pgfnames.o \
../src/common/pgfnames_srv.o \
../src/common/psprintf.o \
../src/common/psprintf_srv.o \
../src/common/relpath.o \
../src/common/relpath_srv.o \
../src/common/rmtree.o \
../src/common/rmtree_srv.o \
../src/common/username.o \
../src/common/username_srv.o \
../src/common/wait_error.o \
../src/common/wait_error_srv.o 

C_SRCS += \
../src/common/exec.c \
../src/common/fe_memutils.c \
../src/common/pgfnames.c \
../src/common/psprintf.c \
../src/common/relpath.c \
../src/common/rmtree.c \
../src/common/username.c \
../src/common/wait_error.c 

OBJS += \
./src/common/exec.o \
./src/common/fe_memutils.o \
./src/common/pgfnames.o \
./src/common/psprintf.o \
./src/common/relpath.o \
./src/common/rmtree.o \
./src/common/username.o \
./src/common/wait_error.o 

C_DEPS += \
./src/common/exec.d \
./src/common/fe_memutils.d \
./src/common/pgfnames.d \
./src/common/psprintf.d \
./src/common/relpath.d \
./src/common/rmtree.d \
./src/common/username.d \
./src/common/wait_error.d 


# Each subdirectory must supply rules for building sources it contributes
src/common/%.o: ../src/common/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


