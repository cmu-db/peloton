################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/bin/pg_dump/common.o \
../src/bin/pg_dump/compress_io.o \
../src/bin/pg_dump/dumputils.o \
../src/bin/pg_dump/keywords.o \
../src/bin/pg_dump/kwlookup.o \
../src/bin/pg_dump/parallel.o \
../src/bin/pg_dump/pg_backup_archiver.o \
../src/bin/pg_dump/pg_backup_custom.o \
../src/bin/pg_dump/pg_backup_db.o \
../src/bin/pg_dump/pg_backup_directory.o \
../src/bin/pg_dump/pg_backup_null.o \
../src/bin/pg_dump/pg_backup_tar.o \
../src/bin/pg_dump/pg_backup_utils.o \
../src/bin/pg_dump/pg_dump.o \
../src/bin/pg_dump/pg_dump_sort.o \
../src/bin/pg_dump/pg_dumpall.o \
../src/bin/pg_dump/pg_restore.o 

C_SRCS += \
../src/bin/pg_dump/common.c \
../src/bin/pg_dump/compress_io.c \
../src/bin/pg_dump/dumputils.c \
../src/bin/pg_dump/keywords.c \
../src/bin/pg_dump/kwlookup.c \
../src/bin/pg_dump/parallel.c \
../src/bin/pg_dump/pg_backup_archiver.c \
../src/bin/pg_dump/pg_backup_custom.c \
../src/bin/pg_dump/pg_backup_db.c \
../src/bin/pg_dump/pg_backup_directory.c \
../src/bin/pg_dump/pg_backup_null.c \
../src/bin/pg_dump/pg_backup_tar.c \
../src/bin/pg_dump/pg_backup_utils.c \
../src/bin/pg_dump/pg_dump.c \
../src/bin/pg_dump/pg_dump_sort.c \
../src/bin/pg_dump/pg_dumpall.c \
../src/bin/pg_dump/pg_restore.c 

OBJS += \
./src/bin/pg_dump/common.o \
./src/bin/pg_dump/compress_io.o \
./src/bin/pg_dump/dumputils.o \
./src/bin/pg_dump/keywords.o \
./src/bin/pg_dump/kwlookup.o \
./src/bin/pg_dump/parallel.o \
./src/bin/pg_dump/pg_backup_archiver.o \
./src/bin/pg_dump/pg_backup_custom.o \
./src/bin/pg_dump/pg_backup_db.o \
./src/bin/pg_dump/pg_backup_directory.o \
./src/bin/pg_dump/pg_backup_null.o \
./src/bin/pg_dump/pg_backup_tar.o \
./src/bin/pg_dump/pg_backup_utils.o \
./src/bin/pg_dump/pg_dump.o \
./src/bin/pg_dump/pg_dump_sort.o \
./src/bin/pg_dump/pg_dumpall.o \
./src/bin/pg_dump/pg_restore.o 

C_DEPS += \
./src/bin/pg_dump/common.d \
./src/bin/pg_dump/compress_io.d \
./src/bin/pg_dump/dumputils.d \
./src/bin/pg_dump/keywords.d \
./src/bin/pg_dump/kwlookup.d \
./src/bin/pg_dump/parallel.d \
./src/bin/pg_dump/pg_backup_archiver.d \
./src/bin/pg_dump/pg_backup_custom.d \
./src/bin/pg_dump/pg_backup_db.d \
./src/bin/pg_dump/pg_backup_directory.d \
./src/bin/pg_dump/pg_backup_null.d \
./src/bin/pg_dump/pg_backup_tar.d \
./src/bin/pg_dump/pg_backup_utils.d \
./src/bin/pg_dump/pg_dump.d \
./src/bin/pg_dump/pg_dump_sort.d \
./src/bin/pg_dump/pg_dumpall.d \
./src/bin/pg_dump/pg_restore.d 


# Each subdirectory must supply rules for building sources it contributes
src/bin/pg_dump/%.o: ../src/bin/pg_dump/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


