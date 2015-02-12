################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/utils/mb/conv.o \
../src/backend/utils/mb/encnames.o \
../src/backend/utils/mb/mbutils.o \
../src/backend/utils/mb/wchar.o \
../src/backend/utils/mb/wstrcmp.o \
../src/backend/utils/mb/wstrncmp.o 

C_SRCS += \
../src/backend/utils/mb/conv.c \
../src/backend/utils/mb/encnames.c \
../src/backend/utils/mb/iso.c \
../src/backend/utils/mb/mbutils.c \
../src/backend/utils/mb/wchar.c \
../src/backend/utils/mb/win1251.c \
../src/backend/utils/mb/win866.c \
../src/backend/utils/mb/wstrcmp.c \
../src/backend/utils/mb/wstrncmp.c 

OBJS += \
./src/backend/utils/mb/conv.o \
./src/backend/utils/mb/encnames.o \
./src/backend/utils/mb/iso.o \
./src/backend/utils/mb/mbutils.o \
./src/backend/utils/mb/wchar.o \
./src/backend/utils/mb/win1251.o \
./src/backend/utils/mb/win866.o \
./src/backend/utils/mb/wstrcmp.o \
./src/backend/utils/mb/wstrncmp.o 

C_DEPS += \
./src/backend/utils/mb/conv.d \
./src/backend/utils/mb/encnames.d \
./src/backend/utils/mb/iso.d \
./src/backend/utils/mb/mbutils.d \
./src/backend/utils/mb/wchar.d \
./src/backend/utils/mb/win1251.d \
./src/backend/utils/mb/win866.d \
./src/backend/utils/mb/wstrcmp.d \
./src/backend/utils/mb/wstrncmp.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/utils/mb/%.o: ../src/backend/utils/mb/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


