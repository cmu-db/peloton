################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/nodes/bitmapset.o \
../src/backend/nodes/copyfuncs.o \
../src/backend/nodes/equalfuncs.o \
../src/backend/nodes/list.o \
../src/backend/nodes/makefuncs.o \
../src/backend/nodes/nodeFuncs.o \
../src/backend/nodes/nodes.o \
../src/backend/nodes/outfuncs.o \
../src/backend/nodes/params.o \
../src/backend/nodes/print.o \
../src/backend/nodes/read.o \
../src/backend/nodes/readfuncs.o \
../src/backend/nodes/tidbitmap.o \
../src/backend/nodes/value.o 

C_SRCS += \
../src/backend/nodes/bitmapset.c \
../src/backend/nodes/copyfuncs.c \
../src/backend/nodes/equalfuncs.c \
../src/backend/nodes/list.c \
../src/backend/nodes/makefuncs.c \
../src/backend/nodes/nodeFuncs.c \
../src/backend/nodes/nodes.c \
../src/backend/nodes/outfuncs.c \
../src/backend/nodes/params.c \
../src/backend/nodes/print.c \
../src/backend/nodes/read.c \
../src/backend/nodes/readfuncs.c \
../src/backend/nodes/tidbitmap.c \
../src/backend/nodes/value.c 

OBJS += \
./src/backend/nodes/bitmapset.o \
./src/backend/nodes/copyfuncs.o \
./src/backend/nodes/equalfuncs.o \
./src/backend/nodes/list.o \
./src/backend/nodes/makefuncs.o \
./src/backend/nodes/nodeFuncs.o \
./src/backend/nodes/nodes.o \
./src/backend/nodes/outfuncs.o \
./src/backend/nodes/params.o \
./src/backend/nodes/print.o \
./src/backend/nodes/read.o \
./src/backend/nodes/readfuncs.o \
./src/backend/nodes/tidbitmap.o \
./src/backend/nodes/value.o 

C_DEPS += \
./src/backend/nodes/bitmapset.d \
./src/backend/nodes/copyfuncs.d \
./src/backend/nodes/equalfuncs.d \
./src/backend/nodes/list.d \
./src/backend/nodes/makefuncs.d \
./src/backend/nodes/nodeFuncs.d \
./src/backend/nodes/nodes.d \
./src/backend/nodes/outfuncs.d \
./src/backend/nodes/params.d \
./src/backend/nodes/print.d \
./src/backend/nodes/read.d \
./src/backend/nodes/readfuncs.d \
./src/backend/nodes/tidbitmap.d \
./src/backend/nodes/value.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/nodes/%.o: ../src/backend/nodes/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


