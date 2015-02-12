################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/test/modules/test_shm_mq/setup.c \
../src/test/modules/test_shm_mq/test.c \
../src/test/modules/test_shm_mq/worker.c 

OBJS += \
./src/test/modules/test_shm_mq/setup.o \
./src/test/modules/test_shm_mq/test.o \
./src/test/modules/test_shm_mq/worker.o 

C_DEPS += \
./src/test/modules/test_shm_mq/setup.d \
./src/test/modules/test_shm_mq/test.d \
./src/test/modules/test_shm_mq/worker.d 


# Each subdirectory must supply rules for building sources it contributes
src/test/modules/test_shm_mq/%.o: ../src/test/modules/test_shm_mq/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


