################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/bin/scripts/clusterdb.o \
../src/bin/scripts/common.o \
../src/bin/scripts/createdb.o \
../src/bin/scripts/createlang.o \
../src/bin/scripts/createuser.o \
../src/bin/scripts/dropdb.o \
../src/bin/scripts/droplang.o \
../src/bin/scripts/dropuser.o \
../src/bin/scripts/dumputils.o \
../src/bin/scripts/keywords.o \
../src/bin/scripts/kwlookup.o \
../src/bin/scripts/mbprint.o \
../src/bin/scripts/pg_isready.o \
../src/bin/scripts/print.o \
../src/bin/scripts/reindexdb.o \
../src/bin/scripts/vacuumdb.o 

C_SRCS += \
../src/bin/scripts/clusterdb.c \
../src/bin/scripts/common.c \
../src/bin/scripts/createdb.c \
../src/bin/scripts/createlang.c \
../src/bin/scripts/createuser.c \
../src/bin/scripts/dropdb.c \
../src/bin/scripts/droplang.c \
../src/bin/scripts/dropuser.c \
../src/bin/scripts/dumputils.c \
../src/bin/scripts/keywords.c \
../src/bin/scripts/kwlookup.c \
../src/bin/scripts/mbprint.c \
../src/bin/scripts/pg_isready.c \
../src/bin/scripts/print.c \
../src/bin/scripts/reindexdb.c \
../src/bin/scripts/vacuumdb.c 

OBJS += \
./src/bin/scripts/clusterdb.o \
./src/bin/scripts/common.o \
./src/bin/scripts/createdb.o \
./src/bin/scripts/createlang.o \
./src/bin/scripts/createuser.o \
./src/bin/scripts/dropdb.o \
./src/bin/scripts/droplang.o \
./src/bin/scripts/dropuser.o \
./src/bin/scripts/dumputils.o \
./src/bin/scripts/keywords.o \
./src/bin/scripts/kwlookup.o \
./src/bin/scripts/mbprint.o \
./src/bin/scripts/pg_isready.o \
./src/bin/scripts/print.o \
./src/bin/scripts/reindexdb.o \
./src/bin/scripts/vacuumdb.o 

C_DEPS += \
./src/bin/scripts/clusterdb.d \
./src/bin/scripts/common.d \
./src/bin/scripts/createdb.d \
./src/bin/scripts/createlang.d \
./src/bin/scripts/createuser.d \
./src/bin/scripts/dropdb.d \
./src/bin/scripts/droplang.d \
./src/bin/scripts/dropuser.d \
./src/bin/scripts/dumputils.d \
./src/bin/scripts/keywords.d \
./src/bin/scripts/kwlookup.d \
./src/bin/scripts/mbprint.d \
./src/bin/scripts/pg_isready.d \
./src/bin/scripts/print.d \
./src/bin/scripts/reindexdb.d \
./src/bin/scripts/vacuumdb.d 


# Each subdirectory must supply rules for building sources it contributes
src/bin/scripts/%.o: ../src/bin/scripts/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


