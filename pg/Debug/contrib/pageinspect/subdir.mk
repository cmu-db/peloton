################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/pageinspect/brinfuncs.c \
../contrib/pageinspect/btreefuncs.c \
../contrib/pageinspect/fsmfuncs.c \
../contrib/pageinspect/ginfuncs.c \
../contrib/pageinspect/heapfuncs.c \
../contrib/pageinspect/rawpage.c 

OBJS += \
./contrib/pageinspect/brinfuncs.o \
./contrib/pageinspect/btreefuncs.o \
./contrib/pageinspect/fsmfuncs.o \
./contrib/pageinspect/ginfuncs.o \
./contrib/pageinspect/heapfuncs.o \
./contrib/pageinspect/rawpage.o 

C_DEPS += \
./contrib/pageinspect/brinfuncs.d \
./contrib/pageinspect/btreefuncs.d \
./contrib/pageinspect/fsmfuncs.d \
./contrib/pageinspect/ginfuncs.d \
./contrib/pageinspect/heapfuncs.d \
./contrib/pageinspect/rawpage.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/pageinspect/%.o: ../contrib/pageinspect/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


