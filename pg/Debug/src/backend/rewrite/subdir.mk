################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/rewrite/rewriteDefine.o \
../src/backend/rewrite/rewriteHandler.o \
../src/backend/rewrite/rewriteManip.o \
../src/backend/rewrite/rewriteRemove.o \
../src/backend/rewrite/rewriteSupport.o \
../src/backend/rewrite/rowsecurity.o 

C_SRCS += \
../src/backend/rewrite/rewriteDefine.c \
../src/backend/rewrite/rewriteHandler.c \
../src/backend/rewrite/rewriteManip.c \
../src/backend/rewrite/rewriteRemove.c \
../src/backend/rewrite/rewriteSupport.c \
../src/backend/rewrite/rowsecurity.c 

OBJS += \
./src/backend/rewrite/rewriteDefine.o \
./src/backend/rewrite/rewriteHandler.o \
./src/backend/rewrite/rewriteManip.o \
./src/backend/rewrite/rewriteRemove.o \
./src/backend/rewrite/rewriteSupport.o \
./src/backend/rewrite/rowsecurity.o 

C_DEPS += \
./src/backend/rewrite/rewriteDefine.d \
./src/backend/rewrite/rewriteHandler.d \
./src/backend/rewrite/rewriteManip.d \
./src/backend/rewrite/rewriteRemove.d \
./src/backend/rewrite/rewriteSupport.d \
./src/backend/rewrite/rowsecurity.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/rewrite/%.o: ../src/backend/rewrite/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


