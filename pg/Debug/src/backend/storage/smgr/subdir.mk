################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/storage/smgr/md.o \
../src/backend/storage/smgr/mm.o \
../src/backend/storage/smgr/smgr.o \
../src/backend/storage/smgr/smgrtype.o 

C_SRCS += \
../src/backend/storage/smgr/md.c \
../src/backend/storage/smgr/mm.c \
../src/backend/storage/smgr/smgr.c \
../src/backend/storage/smgr/smgrtype.c 

OBJS += \
./src/backend/storage/smgr/md.o \
./src/backend/storage/smgr/mm.o \
./src/backend/storage/smgr/smgr.o \
./src/backend/storage/smgr/smgrtype.o 

C_DEPS += \
./src/backend/storage/smgr/md.d \
./src/backend/storage/smgr/mm.d \
./src/backend/storage/smgr/smgr.d \
./src/backend/storage/smgr/smgrtype.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/storage/smgr/%.o: ../src/backend/storage/smgr/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


