################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/bin/psql/command.o \
../src/bin/psql/common.o \
../src/bin/psql/copy.o \
../src/bin/psql/describe.o \
../src/bin/psql/dumputils.o \
../src/bin/psql/help.o \
../src/bin/psql/input.o \
../src/bin/psql/keywords.o \
../src/bin/psql/kwlookup.o \
../src/bin/psql/large_obj.o \
../src/bin/psql/mainloop.o \
../src/bin/psql/mbprint.o \
../src/bin/psql/print.o \
../src/bin/psql/prompt.o \
../src/bin/psql/sql_help.o \
../src/bin/psql/startup.o \
../src/bin/psql/stringutils.o \
../src/bin/psql/tab-complete.o \
../src/bin/psql/variables.o 

C_SRCS += \
../src/bin/psql/command.c \
../src/bin/psql/common.c \
../src/bin/psql/copy.c \
../src/bin/psql/describe.c \
../src/bin/psql/dumputils.c \
../src/bin/psql/help.c \
../src/bin/psql/input.c \
../src/bin/psql/keywords.c \
../src/bin/psql/kwlookup.c \
../src/bin/psql/large_obj.c \
../src/bin/psql/mainloop.c \
../src/bin/psql/mbprint.c \
../src/bin/psql/print.c \
../src/bin/psql/prompt.c \
../src/bin/psql/psqlscan.c \
../src/bin/psql/sql_help.c \
../src/bin/psql/startup.c \
../src/bin/psql/stringutils.c \
../src/bin/psql/tab-complete.c \
../src/bin/psql/variables.c 

OBJS += \
./src/bin/psql/command.o \
./src/bin/psql/common.o \
./src/bin/psql/copy.o \
./src/bin/psql/describe.o \
./src/bin/psql/dumputils.o \
./src/bin/psql/help.o \
./src/bin/psql/input.o \
./src/bin/psql/keywords.o \
./src/bin/psql/kwlookup.o \
./src/bin/psql/large_obj.o \
./src/bin/psql/mainloop.o \
./src/bin/psql/mbprint.o \
./src/bin/psql/print.o \
./src/bin/psql/prompt.o \
./src/bin/psql/psqlscan.o \
./src/bin/psql/sql_help.o \
./src/bin/psql/startup.o \
./src/bin/psql/stringutils.o \
./src/bin/psql/tab-complete.o \
./src/bin/psql/variables.o 

C_DEPS += \
./src/bin/psql/command.d \
./src/bin/psql/common.d \
./src/bin/psql/copy.d \
./src/bin/psql/describe.d \
./src/bin/psql/dumputils.d \
./src/bin/psql/help.d \
./src/bin/psql/input.d \
./src/bin/psql/keywords.d \
./src/bin/psql/kwlookup.d \
./src/bin/psql/large_obj.d \
./src/bin/psql/mainloop.d \
./src/bin/psql/mbprint.d \
./src/bin/psql/print.d \
./src/bin/psql/prompt.d \
./src/bin/psql/psqlscan.d \
./src/bin/psql/sql_help.d \
./src/bin/psql/startup.d \
./src/bin/psql/stringutils.d \
./src/bin/psql/tab-complete.d \
./src/bin/psql/variables.d 


# Each subdirectory must supply rules for building sources it contributes
src/bin/psql/%.o: ../src/bin/psql/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


