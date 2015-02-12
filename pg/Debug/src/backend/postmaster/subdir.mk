################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/postmaster/autovacuum.o \
../src/backend/postmaster/bgworker.o \
../src/backend/postmaster/bgwriter.o \
../src/backend/postmaster/checkpointer.o \
../src/backend/postmaster/fork_process.o \
../src/backend/postmaster/pgarch.o \
../src/backend/postmaster/pgstat.o \
../src/backend/postmaster/postmaster.o \
../src/backend/postmaster/startup.o \
../src/backend/postmaster/syslogger.o \
../src/backend/postmaster/walwriter.o 

C_SRCS += \
../src/backend/postmaster/autovacuum.c \
../src/backend/postmaster/bgworker.c \
../src/backend/postmaster/bgwriter.c \
../src/backend/postmaster/checkpointer.c \
../src/backend/postmaster/fork_process.c \
../src/backend/postmaster/pgarch.c \
../src/backend/postmaster/pgstat.c \
../src/backend/postmaster/postmaster.c \
../src/backend/postmaster/startup.c \
../src/backend/postmaster/syslogger.c \
../src/backend/postmaster/walwriter.c 

OBJS += \
./src/backend/postmaster/autovacuum.o \
./src/backend/postmaster/bgworker.o \
./src/backend/postmaster/bgwriter.o \
./src/backend/postmaster/checkpointer.o \
./src/backend/postmaster/fork_process.o \
./src/backend/postmaster/pgarch.o \
./src/backend/postmaster/pgstat.o \
./src/backend/postmaster/postmaster.o \
./src/backend/postmaster/startup.o \
./src/backend/postmaster/syslogger.o \
./src/backend/postmaster/walwriter.o 

C_DEPS += \
./src/backend/postmaster/autovacuum.d \
./src/backend/postmaster/bgworker.d \
./src/backend/postmaster/bgwriter.d \
./src/backend/postmaster/checkpointer.d \
./src/backend/postmaster/fork_process.d \
./src/backend/postmaster/pgarch.d \
./src/backend/postmaster/pgstat.d \
./src/backend/postmaster/postmaster.d \
./src/backend/postmaster/startup.d \
./src/backend/postmaster/syslogger.d \
./src/backend/postmaster/walwriter.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/postmaster/%.o: ../src/backend/postmaster/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


