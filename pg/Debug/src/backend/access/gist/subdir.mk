################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/access/gist/gist.o \
../src/backend/access/gist/gistbuild.o \
../src/backend/access/gist/gistbuildbuffers.o \
../src/backend/access/gist/gistget.o \
../src/backend/access/gist/gistproc.o \
../src/backend/access/gist/gistscan.o \
../src/backend/access/gist/gistsplit.o \
../src/backend/access/gist/gistutil.o \
../src/backend/access/gist/gistvacuum.o \
../src/backend/access/gist/gistxlog.o 

C_SRCS += \
../src/backend/access/gist/gist.c \
../src/backend/access/gist/gistbuild.c \
../src/backend/access/gist/gistbuildbuffers.c \
../src/backend/access/gist/gistget.c \
../src/backend/access/gist/gistproc.c \
../src/backend/access/gist/gistscan.c \
../src/backend/access/gist/gistsplit.c \
../src/backend/access/gist/gistutil.c \
../src/backend/access/gist/gistvacuum.c \
../src/backend/access/gist/gistxlog.c 

OBJS += \
./src/backend/access/gist/gist.o \
./src/backend/access/gist/gistbuild.o \
./src/backend/access/gist/gistbuildbuffers.o \
./src/backend/access/gist/gistget.o \
./src/backend/access/gist/gistproc.o \
./src/backend/access/gist/gistscan.o \
./src/backend/access/gist/gistsplit.o \
./src/backend/access/gist/gistutil.o \
./src/backend/access/gist/gistvacuum.o \
./src/backend/access/gist/gistxlog.o 

C_DEPS += \
./src/backend/access/gist/gist.d \
./src/backend/access/gist/gistbuild.d \
./src/backend/access/gist/gistbuildbuffers.d \
./src/backend/access/gist/gistget.d \
./src/backend/access/gist/gistproc.d \
./src/backend/access/gist/gistscan.d \
./src/backend/access/gist/gistsplit.d \
./src/backend/access/gist/gistutil.d \
./src/backend/access/gist/gistvacuum.d \
./src/backend/access/gist/gistxlog.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/access/gist/%.o: ../src/backend/access/gist/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


