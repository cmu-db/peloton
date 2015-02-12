################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/intarray/_int_bool.c \
../contrib/intarray/_int_gin.c \
../contrib/intarray/_int_gist.c \
../contrib/intarray/_int_op.c \
../contrib/intarray/_int_tool.c \
../contrib/intarray/_intbig_gist.c 

OBJS += \
./contrib/intarray/_int_bool.o \
./contrib/intarray/_int_gin.o \
./contrib/intarray/_int_gist.o \
./contrib/intarray/_int_op.o \
./contrib/intarray/_int_tool.o \
./contrib/intarray/_intbig_gist.o 

C_DEPS += \
./contrib/intarray/_int_bool.d \
./contrib/intarray/_int_gin.d \
./contrib/intarray/_int_gist.d \
./contrib/intarray/_int_op.d \
./contrib/intarray/_int_tool.d \
./contrib/intarray/_intbig_gist.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/intarray/%.o: ../contrib/intarray/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


