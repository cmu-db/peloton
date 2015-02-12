################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/access/brin/brin.o \
../src/backend/access/brin/brin_minmax.o \
../src/backend/access/brin/brin_pageops.o \
../src/backend/access/brin/brin_revmap.o \
../src/backend/access/brin/brin_tuple.o \
../src/backend/access/brin/brin_xlog.o 

C_SRCS += \
../src/backend/access/brin/brin.c \
../src/backend/access/brin/brin_minmax.c \
../src/backend/access/brin/brin_pageops.c \
../src/backend/access/brin/brin_revmap.c \
../src/backend/access/brin/brin_tuple.c \
../src/backend/access/brin/brin_xlog.c 

OBJS += \
./src/backend/access/brin/brin.o \
./src/backend/access/brin/brin_minmax.o \
./src/backend/access/brin/brin_pageops.o \
./src/backend/access/brin/brin_revmap.o \
./src/backend/access/brin/brin_tuple.o \
./src/backend/access/brin/brin_xlog.o 

C_DEPS += \
./src/backend/access/brin/brin.d \
./src/backend/access/brin/brin_minmax.d \
./src/backend/access/brin/brin_pageops.d \
./src/backend/access/brin/brin_revmap.d \
./src/backend/access/brin/brin_tuple.d \
./src/backend/access/brin/brin_xlog.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/access/brin/%.o: ../src/backend/access/brin/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


