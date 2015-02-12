################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/port/atomics.o \
../src/backend/port/dynloader.o \
../src/backend/port/pg_latch.o \
../src/backend/port/pg_sema.o \
../src/backend/port/pg_shmem.o 

C_SRCS += \
../src/backend/port/atomics.c \
../src/backend/port/dynloader.c \
../src/backend/port/pg_latch.c \
../src/backend/port/pg_sema.c \
../src/backend/port/pg_shmem.c \
../src/backend/port/posix_sema.c \
../src/backend/port/sysv_sema.c \
../src/backend/port/sysv_shmem.c \
../src/backend/port/unix_latch.c \
../src/backend/port/win32_latch.c \
../src/backend/port/win32_sema.c \
../src/backend/port/win32_shmem.c 

OBJS += \
./src/backend/port/atomics.o \
./src/backend/port/dynloader.o \
./src/backend/port/pg_latch.o \
./src/backend/port/pg_sema.o \
./src/backend/port/pg_shmem.o \
./src/backend/port/posix_sema.o \
./src/backend/port/sysv_sema.o \
./src/backend/port/sysv_shmem.o \
./src/backend/port/unix_latch.o \
./src/backend/port/win32_latch.o \
./src/backend/port/win32_sema.o \
./src/backend/port/win32_shmem.o 

C_DEPS += \
./src/backend/port/atomics.d \
./src/backend/port/dynloader.d \
./src/backend/port/pg_latch.d \
./src/backend/port/pg_sema.d \
./src/backend/port/pg_shmem.d \
./src/backend/port/posix_sema.d \
./src/backend/port/sysv_sema.d \
./src/backend/port/sysv_shmem.d \
./src/backend/port/unix_latch.d \
./src/backend/port/win32_latch.d \
./src/backend/port/win32_sema.d \
./src/backend/port/win32_shmem.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/port/%.o: ../src/backend/port/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


