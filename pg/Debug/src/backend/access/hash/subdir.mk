################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/access/hash/hash.o \
../src/backend/access/hash/hashfunc.o \
../src/backend/access/hash/hashinsert.o \
../src/backend/access/hash/hashovfl.o \
../src/backend/access/hash/hashpage.o \
../src/backend/access/hash/hashscan.o \
../src/backend/access/hash/hashsearch.o \
../src/backend/access/hash/hashsort.o \
../src/backend/access/hash/hashutil.o 

C_SRCS += \
../src/backend/access/hash/hash.c \
../src/backend/access/hash/hashfunc.c \
../src/backend/access/hash/hashinsert.c \
../src/backend/access/hash/hashovfl.c \
../src/backend/access/hash/hashpage.c \
../src/backend/access/hash/hashscan.c \
../src/backend/access/hash/hashsearch.c \
../src/backend/access/hash/hashsort.c \
../src/backend/access/hash/hashutil.c 

OBJS += \
./src/backend/access/hash/hash.o \
./src/backend/access/hash/hashfunc.o \
./src/backend/access/hash/hashinsert.o \
./src/backend/access/hash/hashovfl.o \
./src/backend/access/hash/hashpage.o \
./src/backend/access/hash/hashscan.o \
./src/backend/access/hash/hashsearch.o \
./src/backend/access/hash/hashsort.o \
./src/backend/access/hash/hashutil.o 

C_DEPS += \
./src/backend/access/hash/hash.d \
./src/backend/access/hash/hashfunc.d \
./src/backend/access/hash/hashinsert.d \
./src/backend/access/hash/hashovfl.d \
./src/backend/access/hash/hashpage.d \
./src/backend/access/hash/hashscan.d \
./src/backend/access/hash/hashsearch.d \
./src/backend/access/hash/hashsort.d \
./src/backend/access/hash/hashutil.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/access/hash/%.o: ../src/backend/access/hash/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


