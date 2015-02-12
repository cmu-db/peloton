################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/interfaces/libpq/chklocale.o \
../src/interfaces/libpq/encnames.o \
../src/interfaces/libpq/fe-auth.o \
../src/interfaces/libpq/fe-connect.o \
../src/interfaces/libpq/fe-exec.o \
../src/interfaces/libpq/fe-lobj.o \
../src/interfaces/libpq/fe-misc.o \
../src/interfaces/libpq/fe-print.o \
../src/interfaces/libpq/fe-protocol2.o \
../src/interfaces/libpq/fe-protocol3.o \
../src/interfaces/libpq/fe-secure.o \
../src/interfaces/libpq/getpeereid.o \
../src/interfaces/libpq/inet_net_ntop.o \
../src/interfaces/libpq/ip.o \
../src/interfaces/libpq/libpq-events.o \
../src/interfaces/libpq/md5.o \
../src/interfaces/libpq/noblock.o \
../src/interfaces/libpq/pgstrcasecmp.o \
../src/interfaces/libpq/pqexpbuffer.o \
../src/interfaces/libpq/pqsignal.o \
../src/interfaces/libpq/strlcpy.o \
../src/interfaces/libpq/thread.o \
../src/interfaces/libpq/wchar.o 

C_SRCS += \
../src/interfaces/libpq/chklocale.c \
../src/interfaces/libpq/encnames.c \
../src/interfaces/libpq/fe-auth.c \
../src/interfaces/libpq/fe-connect.c \
../src/interfaces/libpq/fe-exec.c \
../src/interfaces/libpq/fe-lobj.c \
../src/interfaces/libpq/fe-misc.c \
../src/interfaces/libpq/fe-print.c \
../src/interfaces/libpq/fe-protocol2.c \
../src/interfaces/libpq/fe-protocol3.c \
../src/interfaces/libpq/fe-secure-openssl.c \
../src/interfaces/libpq/fe-secure.c \
../src/interfaces/libpq/getpeereid.c \
../src/interfaces/libpq/inet_net_ntop.c \
../src/interfaces/libpq/ip.c \
../src/interfaces/libpq/libpq-events.c \
../src/interfaces/libpq/md5.c \
../src/interfaces/libpq/noblock.c \
../src/interfaces/libpq/pgstrcasecmp.c \
../src/interfaces/libpq/pqexpbuffer.c \
../src/interfaces/libpq/pqsignal.c \
../src/interfaces/libpq/pthread-win32.c \
../src/interfaces/libpq/strlcpy.c \
../src/interfaces/libpq/thread.c \
../src/interfaces/libpq/wchar.c \
../src/interfaces/libpq/win32.c 

OBJS += \
./src/interfaces/libpq/chklocale.o \
./src/interfaces/libpq/encnames.o \
./src/interfaces/libpq/fe-auth.o \
./src/interfaces/libpq/fe-connect.o \
./src/interfaces/libpq/fe-exec.o \
./src/interfaces/libpq/fe-lobj.o \
./src/interfaces/libpq/fe-misc.o \
./src/interfaces/libpq/fe-print.o \
./src/interfaces/libpq/fe-protocol2.o \
./src/interfaces/libpq/fe-protocol3.o \
./src/interfaces/libpq/fe-secure-openssl.o \
./src/interfaces/libpq/fe-secure.o \
./src/interfaces/libpq/getpeereid.o \
./src/interfaces/libpq/inet_net_ntop.o \
./src/interfaces/libpq/ip.o \
./src/interfaces/libpq/libpq-events.o \
./src/interfaces/libpq/md5.o \
./src/interfaces/libpq/noblock.o \
./src/interfaces/libpq/pgstrcasecmp.o \
./src/interfaces/libpq/pqexpbuffer.o \
./src/interfaces/libpq/pqsignal.o \
./src/interfaces/libpq/pthread-win32.o \
./src/interfaces/libpq/strlcpy.o \
./src/interfaces/libpq/thread.o \
./src/interfaces/libpq/wchar.o \
./src/interfaces/libpq/win32.o 

C_DEPS += \
./src/interfaces/libpq/chklocale.d \
./src/interfaces/libpq/encnames.d \
./src/interfaces/libpq/fe-auth.d \
./src/interfaces/libpq/fe-connect.d \
./src/interfaces/libpq/fe-exec.d \
./src/interfaces/libpq/fe-lobj.d \
./src/interfaces/libpq/fe-misc.d \
./src/interfaces/libpq/fe-print.d \
./src/interfaces/libpq/fe-protocol2.d \
./src/interfaces/libpq/fe-protocol3.d \
./src/interfaces/libpq/fe-secure-openssl.d \
./src/interfaces/libpq/fe-secure.d \
./src/interfaces/libpq/getpeereid.d \
./src/interfaces/libpq/inet_net_ntop.d \
./src/interfaces/libpq/ip.d \
./src/interfaces/libpq/libpq-events.d \
./src/interfaces/libpq/md5.d \
./src/interfaces/libpq/noblock.d \
./src/interfaces/libpq/pgstrcasecmp.d \
./src/interfaces/libpq/pqexpbuffer.d \
./src/interfaces/libpq/pqsignal.d \
./src/interfaces/libpq/pthread-win32.d \
./src/interfaces/libpq/strlcpy.d \
./src/interfaces/libpq/thread.d \
./src/interfaces/libpq/wchar.d \
./src/interfaces/libpq/win32.d 


# Each subdirectory must supply rules for building sources it contributes
src/interfaces/libpq/%.o: ../src/interfaces/libpq/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


