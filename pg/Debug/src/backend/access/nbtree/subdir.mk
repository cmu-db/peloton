################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/access/nbtree/nbtcompare.o \
../src/backend/access/nbtree/nbtinsert.o \
../src/backend/access/nbtree/nbtpage.o \
../src/backend/access/nbtree/nbtree.o \
../src/backend/access/nbtree/nbtsearch.o \
../src/backend/access/nbtree/nbtsort.o \
../src/backend/access/nbtree/nbtutils.o \
../src/backend/access/nbtree/nbtxlog.o 

C_SRCS += \
../src/backend/access/nbtree/nbtcompare.c \
../src/backend/access/nbtree/nbtinsert.c \
../src/backend/access/nbtree/nbtpage.c \
../src/backend/access/nbtree/nbtree.c \
../src/backend/access/nbtree/nbtsearch.c \
../src/backend/access/nbtree/nbtsort.c \
../src/backend/access/nbtree/nbtutils.c \
../src/backend/access/nbtree/nbtxlog.c 

OBJS += \
./src/backend/access/nbtree/nbtcompare.o \
./src/backend/access/nbtree/nbtinsert.o \
./src/backend/access/nbtree/nbtpage.o \
./src/backend/access/nbtree/nbtree.o \
./src/backend/access/nbtree/nbtsearch.o \
./src/backend/access/nbtree/nbtsort.o \
./src/backend/access/nbtree/nbtutils.o \
./src/backend/access/nbtree/nbtxlog.o 

C_DEPS += \
./src/backend/access/nbtree/nbtcompare.d \
./src/backend/access/nbtree/nbtinsert.d \
./src/backend/access/nbtree/nbtpage.d \
./src/backend/access/nbtree/nbtree.d \
./src/backend/access/nbtree/nbtsearch.d \
./src/backend/access/nbtree/nbtsort.d \
./src/backend/access/nbtree/nbtutils.d \
./src/backend/access/nbtree/nbtxlog.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/access/nbtree/%.o: ../src/backend/access/nbtree/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


