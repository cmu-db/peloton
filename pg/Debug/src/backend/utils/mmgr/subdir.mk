################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/utils/mmgr/aset.o \
../src/backend/utils/mmgr/mcxt.o \
../src/backend/utils/mmgr/mm_test.o \
../src/backend/utils/mmgr/mm_vers.o \
../src/backend/utils/mmgr/portalmem.o \
../src/backend/utils/mmgr/shmctx.o \
../src/backend/utils/mmgr/shmset.o 

C_SRCS += \
../src/backend/utils/mmgr/aset.c \
../src/backend/utils/mmgr/mcxt.c \
../src/backend/utils/mmgr/portalmem.c \
../src/backend/utils/mmgr/shmctx.c \
../src/backend/utils/mmgr/shmset.c 

OBJS += \
./src/backend/utils/mmgr/aset.o \
./src/backend/utils/mmgr/mcxt.o \
./src/backend/utils/mmgr/portalmem.o \
./src/backend/utils/mmgr/shmctx.o \
./src/backend/utils/mmgr/shmset.o 

C_DEPS += \
./src/backend/utils/mmgr/aset.d \
./src/backend/utils/mmgr/mcxt.d \
./src/backend/utils/mmgr/portalmem.d \
./src/backend/utils/mmgr/shmctx.d \
./src/backend/utils/mmgr/shmset.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/utils/mmgr/%.o: ../src/backend/utils/mmgr/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


