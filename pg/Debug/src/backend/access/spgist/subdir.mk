################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/access/spgist/spgdoinsert.o \
../src/backend/access/spgist/spginsert.o \
../src/backend/access/spgist/spgkdtreeproc.o \
../src/backend/access/spgist/spgquadtreeproc.o \
../src/backend/access/spgist/spgscan.o \
../src/backend/access/spgist/spgtextproc.o \
../src/backend/access/spgist/spgutils.o \
../src/backend/access/spgist/spgvacuum.o \
../src/backend/access/spgist/spgxlog.o 

C_SRCS += \
../src/backend/access/spgist/spgdoinsert.c \
../src/backend/access/spgist/spginsert.c \
../src/backend/access/spgist/spgkdtreeproc.c \
../src/backend/access/spgist/spgquadtreeproc.c \
../src/backend/access/spgist/spgscan.c \
../src/backend/access/spgist/spgtextproc.c \
../src/backend/access/spgist/spgutils.c \
../src/backend/access/spgist/spgvacuum.c \
../src/backend/access/spgist/spgxlog.c 

OBJS += \
./src/backend/access/spgist/spgdoinsert.o \
./src/backend/access/spgist/spginsert.o \
./src/backend/access/spgist/spgkdtreeproc.o \
./src/backend/access/spgist/spgquadtreeproc.o \
./src/backend/access/spgist/spgscan.o \
./src/backend/access/spgist/spgtextproc.o \
./src/backend/access/spgist/spgutils.o \
./src/backend/access/spgist/spgvacuum.o \
./src/backend/access/spgist/spgxlog.o 

C_DEPS += \
./src/backend/access/spgist/spgdoinsert.d \
./src/backend/access/spgist/spginsert.d \
./src/backend/access/spgist/spgkdtreeproc.d \
./src/backend/access/spgist/spgquadtreeproc.d \
./src/backend/access/spgist/spgscan.d \
./src/backend/access/spgist/spgtextproc.d \
./src/backend/access/spgist/spgutils.d \
./src/backend/access/spgist/spgvacuum.d \
./src/backend/access/spgist/spgxlog.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/access/spgist/%.o: ../src/backend/access/spgist/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


