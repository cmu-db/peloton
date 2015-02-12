################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/interfaces/ecpg/preproc/c_keywords.o \
../src/interfaces/ecpg/preproc/descriptor.o \
../src/interfaces/ecpg/preproc/ecpg.o \
../src/interfaces/ecpg/preproc/ecpg_keywords.o \
../src/interfaces/ecpg/preproc/keywords.o \
../src/interfaces/ecpg/preproc/kwlookup.o \
../src/interfaces/ecpg/preproc/output.o \
../src/interfaces/ecpg/preproc/parser.o \
../src/interfaces/ecpg/preproc/preproc.o \
../src/interfaces/ecpg/preproc/type.o \
../src/interfaces/ecpg/preproc/variable.o 

C_SRCS += \
../src/interfaces/ecpg/preproc/c_keywords.c \
../src/interfaces/ecpg/preproc/descriptor.c \
../src/interfaces/ecpg/preproc/ecpg.c \
../src/interfaces/ecpg/preproc/ecpg_keywords.c \
../src/interfaces/ecpg/preproc/keywords.c \
../src/interfaces/ecpg/preproc/kwlookup.c \
../src/interfaces/ecpg/preproc/output.c \
../src/interfaces/ecpg/preproc/parser.c \
../src/interfaces/ecpg/preproc/pgc.c \
../src/interfaces/ecpg/preproc/preproc.c \
../src/interfaces/ecpg/preproc/type.c \
../src/interfaces/ecpg/preproc/variable.c 

OBJS += \
./src/interfaces/ecpg/preproc/c_keywords.o \
./src/interfaces/ecpg/preproc/descriptor.o \
./src/interfaces/ecpg/preproc/ecpg.o \
./src/interfaces/ecpg/preproc/ecpg_keywords.o \
./src/interfaces/ecpg/preproc/keywords.o \
./src/interfaces/ecpg/preproc/kwlookup.o \
./src/interfaces/ecpg/preproc/output.o \
./src/interfaces/ecpg/preproc/parser.o \
./src/interfaces/ecpg/preproc/pgc.o \
./src/interfaces/ecpg/preproc/preproc.o \
./src/interfaces/ecpg/preproc/type.o \
./src/interfaces/ecpg/preproc/variable.o 

C_DEPS += \
./src/interfaces/ecpg/preproc/c_keywords.d \
./src/interfaces/ecpg/preproc/descriptor.d \
./src/interfaces/ecpg/preproc/ecpg.d \
./src/interfaces/ecpg/preproc/ecpg_keywords.d \
./src/interfaces/ecpg/preproc/keywords.d \
./src/interfaces/ecpg/preproc/kwlookup.d \
./src/interfaces/ecpg/preproc/output.d \
./src/interfaces/ecpg/preproc/parser.d \
./src/interfaces/ecpg/preproc/pgc.d \
./src/interfaces/ecpg/preproc/preproc.d \
./src/interfaces/ecpg/preproc/type.d \
./src/interfaces/ecpg/preproc/variable.d 


# Each subdirectory must supply rules for building sources it contributes
src/interfaces/ecpg/preproc/%.o: ../src/interfaces/ecpg/preproc/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


