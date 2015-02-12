################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/access/index/genam.o \
../src/backend/access/index/indexam.o 

C_SRCS += \
../src/backend/access/index/genam.c \
../src/backend/access/index/indexam.c 

OBJS += \
./src/backend/access/index/genam.o \
./src/backend/access/index/indexam.o 

C_DEPS += \
./src/backend/access/index/genam.d \
./src/backend/access/index/indexam.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/access/index/%.o: ../src/backend/access/index/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


