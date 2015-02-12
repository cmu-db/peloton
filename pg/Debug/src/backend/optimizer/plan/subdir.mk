################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/optimizer/plan/analyzejoins.o \
../src/backend/optimizer/plan/createplan.o \
../src/backend/optimizer/plan/initsplan.o \
../src/backend/optimizer/plan/planagg.o \
../src/backend/optimizer/plan/planmain.o \
../src/backend/optimizer/plan/planner.o \
../src/backend/optimizer/plan/setrefs.o \
../src/backend/optimizer/plan/subselect.o 

C_SRCS += \
../src/backend/optimizer/plan/analyzejoins.c \
../src/backend/optimizer/plan/createplan.c \
../src/backend/optimizer/plan/initsplan.c \
../src/backend/optimizer/plan/planagg.c \
../src/backend/optimizer/plan/planmain.c \
../src/backend/optimizer/plan/planner.c \
../src/backend/optimizer/plan/setrefs.c \
../src/backend/optimizer/plan/subselect.c 

OBJS += \
./src/backend/optimizer/plan/analyzejoins.o \
./src/backend/optimizer/plan/createplan.o \
./src/backend/optimizer/plan/initsplan.o \
./src/backend/optimizer/plan/planagg.o \
./src/backend/optimizer/plan/planmain.o \
./src/backend/optimizer/plan/planner.o \
./src/backend/optimizer/plan/setrefs.o \
./src/backend/optimizer/plan/subselect.o 

C_DEPS += \
./src/backend/optimizer/plan/analyzejoins.d \
./src/backend/optimizer/plan/createplan.d \
./src/backend/optimizer/plan/initsplan.d \
./src/backend/optimizer/plan/planagg.d \
./src/backend/optimizer/plan/planmain.d \
./src/backend/optimizer/plan/planner.d \
./src/backend/optimizer/plan/setrefs.d \
./src/backend/optimizer/plan/subselect.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/optimizer/plan/%.o: ../src/backend/optimizer/plan/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


