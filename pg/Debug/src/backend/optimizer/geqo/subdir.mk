################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/optimizer/geqo/geqo_copy.o \
../src/backend/optimizer/geqo/geqo_cx.o \
../src/backend/optimizer/geqo/geqo_erx.o \
../src/backend/optimizer/geqo/geqo_eval.o \
../src/backend/optimizer/geqo/geqo_main.o \
../src/backend/optimizer/geqo/geqo_misc.o \
../src/backend/optimizer/geqo/geqo_mutation.o \
../src/backend/optimizer/geqo/geqo_ox1.o \
../src/backend/optimizer/geqo/geqo_ox2.o \
../src/backend/optimizer/geqo/geqo_pmx.o \
../src/backend/optimizer/geqo/geqo_pool.o \
../src/backend/optimizer/geqo/geqo_px.o \
../src/backend/optimizer/geqo/geqo_random.o \
../src/backend/optimizer/geqo/geqo_recombination.o \
../src/backend/optimizer/geqo/geqo_selection.o 

C_SRCS += \
../src/backend/optimizer/geqo/geqo_copy.c \
../src/backend/optimizer/geqo/geqo_cx.c \
../src/backend/optimizer/geqo/geqo_erx.c \
../src/backend/optimizer/geqo/geqo_eval.c \
../src/backend/optimizer/geqo/geqo_main.c \
../src/backend/optimizer/geqo/geqo_misc.c \
../src/backend/optimizer/geqo/geqo_mutation.c \
../src/backend/optimizer/geqo/geqo_ox1.c \
../src/backend/optimizer/geqo/geqo_ox2.c \
../src/backend/optimizer/geqo/geqo_pmx.c \
../src/backend/optimizer/geqo/geqo_pool.c \
../src/backend/optimizer/geqo/geqo_px.c \
../src/backend/optimizer/geqo/geqo_random.c \
../src/backend/optimizer/geqo/geqo_recombination.c \
../src/backend/optimizer/geqo/geqo_selection.c 

OBJS += \
./src/backend/optimizer/geqo/geqo_copy.o \
./src/backend/optimizer/geqo/geqo_cx.o \
./src/backend/optimizer/geqo/geqo_erx.o \
./src/backend/optimizer/geqo/geqo_eval.o \
./src/backend/optimizer/geqo/geqo_main.o \
./src/backend/optimizer/geqo/geqo_misc.o \
./src/backend/optimizer/geqo/geqo_mutation.o \
./src/backend/optimizer/geqo/geqo_ox1.o \
./src/backend/optimizer/geqo/geqo_ox2.o \
./src/backend/optimizer/geqo/geqo_pmx.o \
./src/backend/optimizer/geqo/geqo_pool.o \
./src/backend/optimizer/geqo/geqo_px.o \
./src/backend/optimizer/geqo/geqo_random.o \
./src/backend/optimizer/geqo/geqo_recombination.o \
./src/backend/optimizer/geqo/geqo_selection.o 

C_DEPS += \
./src/backend/optimizer/geqo/geqo_copy.d \
./src/backend/optimizer/geqo/geqo_cx.d \
./src/backend/optimizer/geqo/geqo_erx.d \
./src/backend/optimizer/geqo/geqo_eval.d \
./src/backend/optimizer/geqo/geqo_main.d \
./src/backend/optimizer/geqo/geqo_misc.d \
./src/backend/optimizer/geqo/geqo_mutation.d \
./src/backend/optimizer/geqo/geqo_ox1.d \
./src/backend/optimizer/geqo/geqo_ox2.d \
./src/backend/optimizer/geqo/geqo_pmx.d \
./src/backend/optimizer/geqo/geqo_pool.d \
./src/backend/optimizer/geqo/geqo_px.d \
./src/backend/optimizer/geqo/geqo_random.d \
./src/backend/optimizer/geqo/geqo_recombination.d \
./src/backend/optimizer/geqo/geqo_selection.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/optimizer/geqo/%.o: ../src/backend/optimizer/geqo/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


