################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/optimizer/prep/prepjointree.o \
../src/backend/optimizer/prep/prepqual.o \
../src/backend/optimizer/prep/prepsecurity.o \
../src/backend/optimizer/prep/preptlist.o \
../src/backend/optimizer/prep/prepunion.o 

C_SRCS += \
../src/backend/optimizer/prep/prepjointree.c \
../src/backend/optimizer/prep/prepqual.c \
../src/backend/optimizer/prep/prepsecurity.c \
../src/backend/optimizer/prep/preptlist.c \
../src/backend/optimizer/prep/prepunion.c 

OBJS += \
./src/backend/optimizer/prep/prepjointree.o \
./src/backend/optimizer/prep/prepqual.o \
./src/backend/optimizer/prep/prepsecurity.o \
./src/backend/optimizer/prep/preptlist.o \
./src/backend/optimizer/prep/prepunion.o 

C_DEPS += \
./src/backend/optimizer/prep/prepjointree.d \
./src/backend/optimizer/prep/prepqual.d \
./src/backend/optimizer/prep/prepsecurity.d \
./src/backend/optimizer/prep/preptlist.d \
./src/backend/optimizer/prep/prepunion.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/optimizer/prep/%.o: ../src/backend/optimizer/prep/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


