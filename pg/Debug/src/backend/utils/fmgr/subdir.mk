################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/utils/fmgr/dfmgr.o \
../src/backend/utils/fmgr/fmgr.o \
../src/backend/utils/fmgr/funcapi.o 

C_SRCS += \
../src/backend/utils/fmgr/dfmgr.c \
../src/backend/utils/fmgr/fmgr.c \
../src/backend/utils/fmgr/funcapi.c 

OBJS += \
./src/backend/utils/fmgr/dfmgr.o \
./src/backend/utils/fmgr/fmgr.o \
./src/backend/utils/fmgr/funcapi.o 

C_DEPS += \
./src/backend/utils/fmgr/dfmgr.d \
./src/backend/utils/fmgr/fmgr.d \
./src/backend/utils/fmgr/funcapi.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/utils/fmgr/%.o: ../src/backend/utils/fmgr/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


