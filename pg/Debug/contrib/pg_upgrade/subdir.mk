################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/pg_upgrade/check.c \
../contrib/pg_upgrade/controldata.c \
../contrib/pg_upgrade/dump.c \
../contrib/pg_upgrade/exec.c \
../contrib/pg_upgrade/file.c \
../contrib/pg_upgrade/function.c \
../contrib/pg_upgrade/info.c \
../contrib/pg_upgrade/option.c \
../contrib/pg_upgrade/page.c \
../contrib/pg_upgrade/parallel.c \
../contrib/pg_upgrade/pg_upgrade.c \
../contrib/pg_upgrade/relfilenode.c \
../contrib/pg_upgrade/server.c \
../contrib/pg_upgrade/tablespace.c \
../contrib/pg_upgrade/util.c \
../contrib/pg_upgrade/version.c 

OBJS += \
./contrib/pg_upgrade/check.o \
./contrib/pg_upgrade/controldata.o \
./contrib/pg_upgrade/dump.o \
./contrib/pg_upgrade/exec.o \
./contrib/pg_upgrade/file.o \
./contrib/pg_upgrade/function.o \
./contrib/pg_upgrade/info.o \
./contrib/pg_upgrade/option.o \
./contrib/pg_upgrade/page.o \
./contrib/pg_upgrade/parallel.o \
./contrib/pg_upgrade/pg_upgrade.o \
./contrib/pg_upgrade/relfilenode.o \
./contrib/pg_upgrade/server.o \
./contrib/pg_upgrade/tablespace.o \
./contrib/pg_upgrade/util.o \
./contrib/pg_upgrade/version.o 

C_DEPS += \
./contrib/pg_upgrade/check.d \
./contrib/pg_upgrade/controldata.d \
./contrib/pg_upgrade/dump.d \
./contrib/pg_upgrade/exec.d \
./contrib/pg_upgrade/file.d \
./contrib/pg_upgrade/function.d \
./contrib/pg_upgrade/info.d \
./contrib/pg_upgrade/option.d \
./contrib/pg_upgrade/page.d \
./contrib/pg_upgrade/parallel.d \
./contrib/pg_upgrade/pg_upgrade.d \
./contrib/pg_upgrade/relfilenode.d \
./contrib/pg_upgrade/server.d \
./contrib/pg_upgrade/tablespace.d \
./contrib/pg_upgrade/util.d \
./contrib/pg_upgrade/version.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/pg_upgrade/%.o: ../contrib/pg_upgrade/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


