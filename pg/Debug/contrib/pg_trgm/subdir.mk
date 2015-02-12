################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/pg_trgm/trgm_gin.c \
../contrib/pg_trgm/trgm_gist.c \
../contrib/pg_trgm/trgm_op.c \
../contrib/pg_trgm/trgm_regexp.c 

OBJS += \
./contrib/pg_trgm/trgm_gin.o \
./contrib/pg_trgm/trgm_gist.o \
./contrib/pg_trgm/trgm_op.o \
./contrib/pg_trgm/trgm_regexp.o 

C_DEPS += \
./contrib/pg_trgm/trgm_gin.d \
./contrib/pg_trgm/trgm_gist.d \
./contrib/pg_trgm/trgm_op.d \
./contrib/pg_trgm/trgm_regexp.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/pg_trgm/%.o: ../contrib/pg_trgm/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


