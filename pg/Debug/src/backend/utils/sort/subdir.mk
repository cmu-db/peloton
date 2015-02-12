################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/utils/sort/logtape.o \
../src/backend/utils/sort/sortsupport.o \
../src/backend/utils/sort/tuplesort.o \
../src/backend/utils/sort/tuplestore.o 

C_SRCS += \
../src/backend/utils/sort/logtape.c \
../src/backend/utils/sort/qsort_tuple.c \
../src/backend/utils/sort/sortsupport.c \
../src/backend/utils/sort/tuplesort.c \
../src/backend/utils/sort/tuplestore.c 

OBJS += \
./src/backend/utils/sort/logtape.o \
./src/backend/utils/sort/qsort_tuple.o \
./src/backend/utils/sort/sortsupport.o \
./src/backend/utils/sort/tuplesort.o \
./src/backend/utils/sort/tuplestore.o 

C_DEPS += \
./src/backend/utils/sort/logtape.d \
./src/backend/utils/sort/qsort_tuple.d \
./src/backend/utils/sort/sortsupport.d \
./src/backend/utils/sort/tuplesort.d \
./src/backend/utils/sort/tuplestore.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/utils/sort/%.o: ../src/backend/utils/sort/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


