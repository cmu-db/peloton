################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/storage/page/bufpage.o \
../src/backend/storage/page/checksum.o \
../src/backend/storage/page/itemptr.o 

C_SRCS += \
../src/backend/storage/page/bufpage.c \
../src/backend/storage/page/checksum.c \
../src/backend/storage/page/itemptr.c 

OBJS += \
./src/backend/storage/page/bufpage.o \
./src/backend/storage/page/checksum.o \
./src/backend/storage/page/itemptr.o 

C_DEPS += \
./src/backend/storage/page/bufpage.d \
./src/backend/storage/page/checksum.d \
./src/backend/storage/page/itemptr.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/storage/page/%.o: ../src/backend/storage/page/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


