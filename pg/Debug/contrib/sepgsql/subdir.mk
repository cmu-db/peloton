################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/sepgsql/database.c \
../contrib/sepgsql/dml.c \
../contrib/sepgsql/hooks.c \
../contrib/sepgsql/label.c \
../contrib/sepgsql/proc.c \
../contrib/sepgsql/relation.c \
../contrib/sepgsql/schema.c \
../contrib/sepgsql/selinux.c \
../contrib/sepgsql/uavc.c 

OBJS += \
./contrib/sepgsql/database.o \
./contrib/sepgsql/dml.o \
./contrib/sepgsql/hooks.o \
./contrib/sepgsql/label.o \
./contrib/sepgsql/proc.o \
./contrib/sepgsql/relation.o \
./contrib/sepgsql/schema.o \
./contrib/sepgsql/selinux.o \
./contrib/sepgsql/uavc.o 

C_DEPS += \
./contrib/sepgsql/database.d \
./contrib/sepgsql/dml.d \
./contrib/sepgsql/hooks.d \
./contrib/sepgsql/label.d \
./contrib/sepgsql/proc.d \
./contrib/sepgsql/relation.d \
./contrib/sepgsql/schema.d \
./contrib/sepgsql/selinux.d \
./contrib/sepgsql/uavc.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/sepgsql/%.o: ../contrib/sepgsql/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


