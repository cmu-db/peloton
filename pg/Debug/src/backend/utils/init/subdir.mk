################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/utils/init/globals.o \
../src/backend/utils/init/miscinit.o \
../src/backend/utils/init/postinit.o 

C_SRCS += \
../src/backend/utils/init/globals.c \
../src/backend/utils/init/miscinit.c \
../src/backend/utils/init/postinit.c 

OBJS += \
./src/backend/utils/init/globals.o \
./src/backend/utils/init/miscinit.o \
./src/backend/utils/init/postinit.o 

C_DEPS += \
./src/backend/utils/init/globals.d \
./src/backend/utils/init/miscinit.d \
./src/backend/utils/init/postinit.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/utils/init/%.o: ../src/backend/utils/init/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


