################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/utils/time/combocid.o \
../src/backend/utils/time/snapmgr.o \
../src/backend/utils/time/tqual.o 

C_SRCS += \
../src/backend/utils/time/combocid.c \
../src/backend/utils/time/snapmgr.c \
../src/backend/utils/time/tqual.c 

OBJS += \
./src/backend/utils/time/combocid.o \
./src/backend/utils/time/snapmgr.o \
./src/backend/utils/time/tqual.o 

C_DEPS += \
./src/backend/utils/time/combocid.d \
./src/backend/utils/time/snapmgr.d \
./src/backend/utils/time/tqual.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/utils/time/%.o: ../src/backend/utils/time/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


