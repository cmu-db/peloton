################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/btree_gin/btree_gin.c 

OBJS += \
./contrib/btree_gin/btree_gin.o 

C_DEPS += \
./contrib/btree_gin/btree_gin.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/btree_gin/%.o: ../contrib/btree_gin/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


