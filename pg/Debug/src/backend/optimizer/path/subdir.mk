################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/optimizer/path/allpaths.o \
../src/backend/optimizer/path/clausesel.o \
../src/backend/optimizer/path/costsize.o \
../src/backend/optimizer/path/equivclass.o \
../src/backend/optimizer/path/indxpath.o \
../src/backend/optimizer/path/joinpath.o \
../src/backend/optimizer/path/joinrels.o \
../src/backend/optimizer/path/pathkeys.o \
../src/backend/optimizer/path/tidpath.o 

C_SRCS += \
../src/backend/optimizer/path/allpaths.c \
../src/backend/optimizer/path/clausesel.c \
../src/backend/optimizer/path/costsize.c \
../src/backend/optimizer/path/equivclass.c \
../src/backend/optimizer/path/indxpath.c \
../src/backend/optimizer/path/joinpath.c \
../src/backend/optimizer/path/joinrels.c \
../src/backend/optimizer/path/pathkeys.c \
../src/backend/optimizer/path/tidpath.c 

OBJS += \
./src/backend/optimizer/path/allpaths.o \
./src/backend/optimizer/path/clausesel.o \
./src/backend/optimizer/path/costsize.o \
./src/backend/optimizer/path/equivclass.o \
./src/backend/optimizer/path/indxpath.o \
./src/backend/optimizer/path/joinpath.o \
./src/backend/optimizer/path/joinrels.o \
./src/backend/optimizer/path/pathkeys.o \
./src/backend/optimizer/path/tidpath.o 

C_DEPS += \
./src/backend/optimizer/path/allpaths.d \
./src/backend/optimizer/path/clausesel.d \
./src/backend/optimizer/path/costsize.d \
./src/backend/optimizer/path/equivclass.d \
./src/backend/optimizer/path/indxpath.d \
./src/backend/optimizer/path/joinpath.d \
./src/backend/optimizer/path/joinrels.d \
./src/backend/optimizer/path/pathkeys.d \
./src/backend/optimizer/path/tidpath.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/optimizer/path/%.o: ../src/backend/optimizer/path/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


