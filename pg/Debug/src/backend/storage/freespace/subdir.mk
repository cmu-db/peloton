################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/storage/freespace/freespace.o \
../src/backend/storage/freespace/fsmpage.o \
../src/backend/storage/freespace/indexfsm.o 

C_SRCS += \
../src/backend/storage/freespace/freespace.c \
../src/backend/storage/freespace/fsmpage.c \
../src/backend/storage/freespace/indexfsm.c 

OBJS += \
./src/backend/storage/freespace/freespace.o \
./src/backend/storage/freespace/fsmpage.o \
./src/backend/storage/freespace/indexfsm.o 

C_DEPS += \
./src/backend/storage/freespace/freespace.d \
./src/backend/storage/freespace/fsmpage.d \
./src/backend/storage/freespace/indexfsm.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/storage/freespace/%.o: ../src/backend/storage/freespace/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


