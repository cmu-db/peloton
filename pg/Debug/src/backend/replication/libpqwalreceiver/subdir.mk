################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/replication/libpqwalreceiver/libpqwalreceiver.o 

C_SRCS += \
../src/backend/replication/libpqwalreceiver/libpqwalreceiver.c 

OBJS += \
./src/backend/replication/libpqwalreceiver/libpqwalreceiver.o 

C_DEPS += \
./src/backend/replication/libpqwalreceiver/libpqwalreceiver.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/replication/libpqwalreceiver/%.o: ../src/backend/replication/libpqwalreceiver/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


