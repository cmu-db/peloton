################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/access/gin/ginarrayproc.o \
../src/backend/access/gin/ginbtree.o \
../src/backend/access/gin/ginbulk.o \
../src/backend/access/gin/gindatapage.o \
../src/backend/access/gin/ginentrypage.o \
../src/backend/access/gin/ginfast.o \
../src/backend/access/gin/ginget.o \
../src/backend/access/gin/gininsert.o \
../src/backend/access/gin/ginlogic.o \
../src/backend/access/gin/ginpostinglist.o \
../src/backend/access/gin/ginscan.o \
../src/backend/access/gin/ginutil.o \
../src/backend/access/gin/ginvacuum.o \
../src/backend/access/gin/ginxlog.o 

C_SRCS += \
../src/backend/access/gin/ginarrayproc.c \
../src/backend/access/gin/ginbtree.c \
../src/backend/access/gin/ginbulk.c \
../src/backend/access/gin/gindatapage.c \
../src/backend/access/gin/ginentrypage.c \
../src/backend/access/gin/ginfast.c \
../src/backend/access/gin/ginget.c \
../src/backend/access/gin/gininsert.c \
../src/backend/access/gin/ginlogic.c \
../src/backend/access/gin/ginpostinglist.c \
../src/backend/access/gin/ginscan.c \
../src/backend/access/gin/ginutil.c \
../src/backend/access/gin/ginvacuum.c \
../src/backend/access/gin/ginxlog.c 

OBJS += \
./src/backend/access/gin/ginarrayproc.o \
./src/backend/access/gin/ginbtree.o \
./src/backend/access/gin/ginbulk.o \
./src/backend/access/gin/gindatapage.o \
./src/backend/access/gin/ginentrypage.o \
./src/backend/access/gin/ginfast.o \
./src/backend/access/gin/ginget.o \
./src/backend/access/gin/gininsert.o \
./src/backend/access/gin/ginlogic.o \
./src/backend/access/gin/ginpostinglist.o \
./src/backend/access/gin/ginscan.o \
./src/backend/access/gin/ginutil.o \
./src/backend/access/gin/ginvacuum.o \
./src/backend/access/gin/ginxlog.o 

C_DEPS += \
./src/backend/access/gin/ginarrayproc.d \
./src/backend/access/gin/ginbtree.d \
./src/backend/access/gin/ginbulk.d \
./src/backend/access/gin/gindatapage.d \
./src/backend/access/gin/ginentrypage.d \
./src/backend/access/gin/ginfast.d \
./src/backend/access/gin/ginget.d \
./src/backend/access/gin/gininsert.d \
./src/backend/access/gin/ginlogic.d \
./src/backend/access/gin/ginpostinglist.d \
./src/backend/access/gin/ginscan.d \
./src/backend/access/gin/ginutil.d \
./src/backend/access/gin/ginvacuum.d \
./src/backend/access/gin/ginxlog.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/access/gin/%.o: ../src/backend/access/gin/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


