################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/access/rmgrdesc/brindesc.o \
../src/backend/access/rmgrdesc/clogdesc.o \
../src/backend/access/rmgrdesc/committsdesc.o \
../src/backend/access/rmgrdesc/dbasedesc.o \
../src/backend/access/rmgrdesc/gindesc.o \
../src/backend/access/rmgrdesc/gistdesc.o \
../src/backend/access/rmgrdesc/hashdesc.o \
../src/backend/access/rmgrdesc/heapdesc.o \
../src/backend/access/rmgrdesc/mxactdesc.o \
../src/backend/access/rmgrdesc/nbtdesc.o \
../src/backend/access/rmgrdesc/relmapdesc.o \
../src/backend/access/rmgrdesc/seqdesc.o \
../src/backend/access/rmgrdesc/smgrdesc.o \
../src/backend/access/rmgrdesc/spgdesc.o \
../src/backend/access/rmgrdesc/standbydesc.o \
../src/backend/access/rmgrdesc/tblspcdesc.o \
../src/backend/access/rmgrdesc/xactdesc.o \
../src/backend/access/rmgrdesc/xlogdesc.o 

C_SRCS += \
../src/backend/access/rmgrdesc/brindesc.c \
../src/backend/access/rmgrdesc/clogdesc.c \
../src/backend/access/rmgrdesc/committsdesc.c \
../src/backend/access/rmgrdesc/dbasedesc.c \
../src/backend/access/rmgrdesc/gindesc.c \
../src/backend/access/rmgrdesc/gistdesc.c \
../src/backend/access/rmgrdesc/hashdesc.c \
../src/backend/access/rmgrdesc/heapdesc.c \
../src/backend/access/rmgrdesc/mxactdesc.c \
../src/backend/access/rmgrdesc/nbtdesc.c \
../src/backend/access/rmgrdesc/relmapdesc.c \
../src/backend/access/rmgrdesc/seqdesc.c \
../src/backend/access/rmgrdesc/smgrdesc.c \
../src/backend/access/rmgrdesc/spgdesc.c \
../src/backend/access/rmgrdesc/standbydesc.c \
../src/backend/access/rmgrdesc/tblspcdesc.c \
../src/backend/access/rmgrdesc/xactdesc.c \
../src/backend/access/rmgrdesc/xlogdesc.c 

OBJS += \
./src/backend/access/rmgrdesc/brindesc.o \
./src/backend/access/rmgrdesc/clogdesc.o \
./src/backend/access/rmgrdesc/committsdesc.o \
./src/backend/access/rmgrdesc/dbasedesc.o \
./src/backend/access/rmgrdesc/gindesc.o \
./src/backend/access/rmgrdesc/gistdesc.o \
./src/backend/access/rmgrdesc/hashdesc.o \
./src/backend/access/rmgrdesc/heapdesc.o \
./src/backend/access/rmgrdesc/mxactdesc.o \
./src/backend/access/rmgrdesc/nbtdesc.o \
./src/backend/access/rmgrdesc/relmapdesc.o \
./src/backend/access/rmgrdesc/seqdesc.o \
./src/backend/access/rmgrdesc/smgrdesc.o \
./src/backend/access/rmgrdesc/spgdesc.o \
./src/backend/access/rmgrdesc/standbydesc.o \
./src/backend/access/rmgrdesc/tblspcdesc.o \
./src/backend/access/rmgrdesc/xactdesc.o \
./src/backend/access/rmgrdesc/xlogdesc.o 

C_DEPS += \
./src/backend/access/rmgrdesc/brindesc.d \
./src/backend/access/rmgrdesc/clogdesc.d \
./src/backend/access/rmgrdesc/committsdesc.d \
./src/backend/access/rmgrdesc/dbasedesc.d \
./src/backend/access/rmgrdesc/gindesc.d \
./src/backend/access/rmgrdesc/gistdesc.d \
./src/backend/access/rmgrdesc/hashdesc.d \
./src/backend/access/rmgrdesc/heapdesc.d \
./src/backend/access/rmgrdesc/mxactdesc.d \
./src/backend/access/rmgrdesc/nbtdesc.d \
./src/backend/access/rmgrdesc/relmapdesc.d \
./src/backend/access/rmgrdesc/seqdesc.d \
./src/backend/access/rmgrdesc/smgrdesc.d \
./src/backend/access/rmgrdesc/spgdesc.d \
./src/backend/access/rmgrdesc/standbydesc.d \
./src/backend/access/rmgrdesc/tblspcdesc.d \
./src/backend/access/rmgrdesc/xactdesc.d \
./src/backend/access/rmgrdesc/xlogdesc.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/access/rmgrdesc/%.o: ../src/backend/access/rmgrdesc/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


