################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/optimizer/util/clauses.o \
../src/backend/optimizer/util/joininfo.o \
../src/backend/optimizer/util/orclauses.o \
../src/backend/optimizer/util/pathnode.o \
../src/backend/optimizer/util/placeholder.o \
../src/backend/optimizer/util/plancat.o \
../src/backend/optimizer/util/predtest.o \
../src/backend/optimizer/util/relnode.o \
../src/backend/optimizer/util/restrictinfo.o \
../src/backend/optimizer/util/tlist.o \
../src/backend/optimizer/util/var.o 

C_SRCS += \
../src/backend/optimizer/util/clauses.c \
../src/backend/optimizer/util/joininfo.c \
../src/backend/optimizer/util/orclauses.c \
../src/backend/optimizer/util/pathnode.c \
../src/backend/optimizer/util/placeholder.c \
../src/backend/optimizer/util/plancat.c \
../src/backend/optimizer/util/predtest.c \
../src/backend/optimizer/util/relnode.c \
../src/backend/optimizer/util/restrictinfo.c \
../src/backend/optimizer/util/tlist.c \
../src/backend/optimizer/util/var.c 

OBJS += \
./src/backend/optimizer/util/clauses.o \
./src/backend/optimizer/util/joininfo.o \
./src/backend/optimizer/util/orclauses.o \
./src/backend/optimizer/util/pathnode.o \
./src/backend/optimizer/util/placeholder.o \
./src/backend/optimizer/util/plancat.o \
./src/backend/optimizer/util/predtest.o \
./src/backend/optimizer/util/relnode.o \
./src/backend/optimizer/util/restrictinfo.o \
./src/backend/optimizer/util/tlist.o \
./src/backend/optimizer/util/var.o 

C_DEPS += \
./src/backend/optimizer/util/clauses.d \
./src/backend/optimizer/util/joininfo.d \
./src/backend/optimizer/util/orclauses.d \
./src/backend/optimizer/util/pathnode.d \
./src/backend/optimizer/util/placeholder.d \
./src/backend/optimizer/util/plancat.d \
./src/backend/optimizer/util/predtest.d \
./src/backend/optimizer/util/relnode.d \
./src/backend/optimizer/util/restrictinfo.d \
./src/backend/optimizer/util/tlist.d \
./src/backend/optimizer/util/var.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/optimizer/util/%.o: ../src/backend/optimizer/util/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


