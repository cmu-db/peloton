################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/replication/basebackup.o \
../src/backend/replication/repl_gram.o \
../src/backend/replication/slot.o \
../src/backend/replication/slotfuncs.o \
../src/backend/replication/syncrep.o \
../src/backend/replication/walreceiver.o \
../src/backend/replication/walreceiverfuncs.o \
../src/backend/replication/walsender.o 

C_SRCS += \
../src/backend/replication/basebackup.c \
../src/backend/replication/repl_gram.c \
../src/backend/replication/repl_scanner.c \
../src/backend/replication/slot.c \
../src/backend/replication/slotfuncs.c \
../src/backend/replication/syncrep.c \
../src/backend/replication/walreceiver.c \
../src/backend/replication/walreceiverfuncs.c \
../src/backend/replication/walsender.c 

OBJS += \
./src/backend/replication/basebackup.o \
./src/backend/replication/repl_gram.o \
./src/backend/replication/repl_scanner.o \
./src/backend/replication/slot.o \
./src/backend/replication/slotfuncs.o \
./src/backend/replication/syncrep.o \
./src/backend/replication/walreceiver.o \
./src/backend/replication/walreceiverfuncs.o \
./src/backend/replication/walsender.o 

C_DEPS += \
./src/backend/replication/basebackup.d \
./src/backend/replication/repl_gram.d \
./src/backend/replication/repl_scanner.d \
./src/backend/replication/slot.d \
./src/backend/replication/slotfuncs.d \
./src/backend/replication/syncrep.d \
./src/backend/replication/walreceiver.d \
./src/backend/replication/walreceiverfuncs.d \
./src/backend/replication/walsender.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/replication/%.o: ../src/backend/replication/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


