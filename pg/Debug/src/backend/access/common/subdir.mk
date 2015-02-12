################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/access/common/heaptuple.o \
../src/backend/access/common/indextuple.o \
../src/backend/access/common/printtup.o \
../src/backend/access/common/reloptions.o \
../src/backend/access/common/scankey.o \
../src/backend/access/common/tupconvert.o \
../src/backend/access/common/tupdesc.o 

C_SRCS += \
../src/backend/access/common/heaptuple.c \
../src/backend/access/common/indextuple.c \
../src/backend/access/common/printtup.c \
../src/backend/access/common/reloptions.c \
../src/backend/access/common/scankey.c \
../src/backend/access/common/tupconvert.c \
../src/backend/access/common/tupdesc.c 

OBJS += \
./src/backend/access/common/heaptuple.o \
./src/backend/access/common/indextuple.o \
./src/backend/access/common/printtup.o \
./src/backend/access/common/reloptions.o \
./src/backend/access/common/scankey.o \
./src/backend/access/common/tupconvert.o \
./src/backend/access/common/tupdesc.o 

C_DEPS += \
./src/backend/access/common/heaptuple.d \
./src/backend/access/common/indextuple.d \
./src/backend/access/common/printtup.d \
./src/backend/access/common/reloptions.d \
./src/backend/access/common/scankey.d \
./src/backend/access/common/tupconvert.d \
./src/backend/access/common/tupdesc.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/access/common/%.o: ../src/backend/access/common/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


