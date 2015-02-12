################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/ltree/_ltree_gist.c \
../contrib/ltree/_ltree_op.c \
../contrib/ltree/crc32.c \
../contrib/ltree/lquery_op.c \
../contrib/ltree/ltree_gist.c \
../contrib/ltree/ltree_io.c \
../contrib/ltree/ltree_op.c \
../contrib/ltree/ltxtquery_io.c \
../contrib/ltree/ltxtquery_op.c 

OBJS += \
./contrib/ltree/_ltree_gist.o \
./contrib/ltree/_ltree_op.o \
./contrib/ltree/crc32.o \
./contrib/ltree/lquery_op.o \
./contrib/ltree/ltree_gist.o \
./contrib/ltree/ltree_io.o \
./contrib/ltree/ltree_op.o \
./contrib/ltree/ltxtquery_io.o \
./contrib/ltree/ltxtquery_op.o 

C_DEPS += \
./contrib/ltree/_ltree_gist.d \
./contrib/ltree/_ltree_op.d \
./contrib/ltree/crc32.d \
./contrib/ltree/lquery_op.d \
./contrib/ltree/ltree_gist.d \
./contrib/ltree/ltree_io.d \
./contrib/ltree/ltree_op.d \
./contrib/ltree/ltxtquery_io.d \
./contrib/ltree/ltxtquery_op.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/ltree/%.o: ../contrib/ltree/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


