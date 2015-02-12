################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/utils/misc/guc.o \
../src/backend/utils/misc/help_config.o \
../src/backend/utils/misc/pg_rusage.o \
../src/backend/utils/misc/ps_status.o \
../src/backend/utils/misc/rbtree.o \
../src/backend/utils/misc/superuser.o \
../src/backend/utils/misc/timeout.o \
../src/backend/utils/misc/tzparser.o 

C_SRCS += \
../src/backend/utils/misc/guc-file.c \
../src/backend/utils/misc/guc.c \
../src/backend/utils/misc/help_config.c \
../src/backend/utils/misc/pg_rusage.c \
../src/backend/utils/misc/ps_status.c \
../src/backend/utils/misc/rbtree.c \
../src/backend/utils/misc/superuser.c \
../src/backend/utils/misc/timeout.c \
../src/backend/utils/misc/tzparser.c 

OBJS += \
./src/backend/utils/misc/guc-file.o \
./src/backend/utils/misc/guc.o \
./src/backend/utils/misc/help_config.o \
./src/backend/utils/misc/pg_rusage.o \
./src/backend/utils/misc/ps_status.o \
./src/backend/utils/misc/rbtree.o \
./src/backend/utils/misc/superuser.o \
./src/backend/utils/misc/timeout.o \
./src/backend/utils/misc/tzparser.o 

C_DEPS += \
./src/backend/utils/misc/guc-file.d \
./src/backend/utils/misc/guc.d \
./src/backend/utils/misc/help_config.d \
./src/backend/utils/misc/pg_rusage.d \
./src/backend/utils/misc/ps_status.d \
./src/backend/utils/misc/rbtree.d \
./src/backend/utils/misc/superuser.d \
./src/backend/utils/misc/timeout.d \
./src/backend/utils/misc/tzparser.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/utils/misc/%.o: ../src/backend/utils/misc/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


