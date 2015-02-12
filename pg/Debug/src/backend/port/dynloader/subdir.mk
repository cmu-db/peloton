################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/backend/port/dynloader/aix.c \
../src/backend/port/dynloader/cygwin.c \
../src/backend/port/dynloader/darwin.c \
../src/backend/port/dynloader/freebsd.c \
../src/backend/port/dynloader/hpux.c \
../src/backend/port/dynloader/linux.c \
../src/backend/port/dynloader/netbsd.c \
../src/backend/port/dynloader/openbsd.c \
../src/backend/port/dynloader/sco.c \
../src/backend/port/dynloader/solaris.c \
../src/backend/port/dynloader/unixware.c \
../src/backend/port/dynloader/win32.c 

OBJS += \
./src/backend/port/dynloader/aix.o \
./src/backend/port/dynloader/cygwin.o \
./src/backend/port/dynloader/darwin.o \
./src/backend/port/dynloader/freebsd.o \
./src/backend/port/dynloader/hpux.o \
./src/backend/port/dynloader/linux.o \
./src/backend/port/dynloader/netbsd.o \
./src/backend/port/dynloader/openbsd.o \
./src/backend/port/dynloader/sco.o \
./src/backend/port/dynloader/solaris.o \
./src/backend/port/dynloader/unixware.o \
./src/backend/port/dynloader/win32.o 

C_DEPS += \
./src/backend/port/dynloader/aix.d \
./src/backend/port/dynloader/cygwin.d \
./src/backend/port/dynloader/darwin.d \
./src/backend/port/dynloader/freebsd.d \
./src/backend/port/dynloader/hpux.d \
./src/backend/port/dynloader/linux.d \
./src/backend/port/dynloader/netbsd.d \
./src/backend/port/dynloader/openbsd.d \
./src/backend/port/dynloader/sco.d \
./src/backend/port/dynloader/solaris.d \
./src/backend/port/dynloader/unixware.d \
./src/backend/port/dynloader/win32.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/port/dynloader/%.o: ../src/backend/port/dynloader/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


