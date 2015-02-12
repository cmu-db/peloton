################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/catalog/aclchk.o \
../src/backend/catalog/catalog.o \
../src/backend/catalog/dependency.o \
../src/backend/catalog/heap.o \
../src/backend/catalog/index.o \
../src/backend/catalog/indexing.o \
../src/backend/catalog/namespace.o \
../src/backend/catalog/objectaccess.o \
../src/backend/catalog/objectaddress.o \
../src/backend/catalog/pg_aggregate.o \
../src/backend/catalog/pg_collation.o \
../src/backend/catalog/pg_constraint.o \
../src/backend/catalog/pg_conversion.o \
../src/backend/catalog/pg_db_role_setting.o \
../src/backend/catalog/pg_depend.o \
../src/backend/catalog/pg_enum.o \
../src/backend/catalog/pg_inherits.o \
../src/backend/catalog/pg_largeobject.o \
../src/backend/catalog/pg_namespace.o \
../src/backend/catalog/pg_operator.o \
../src/backend/catalog/pg_proc.o \
../src/backend/catalog/pg_range.o \
../src/backend/catalog/pg_shdepend.o \
../src/backend/catalog/pg_type.o \
../src/backend/catalog/storage.o \
../src/backend/catalog/toasting.o 

C_SRCS += \
../src/backend/catalog/aclchk.c \
../src/backend/catalog/catalog.c \
../src/backend/catalog/dependency.c \
../src/backend/catalog/heap.c \
../src/backend/catalog/index.c \
../src/backend/catalog/indexing.c \
../src/backend/catalog/namespace.c \
../src/backend/catalog/objectaccess.c \
../src/backend/catalog/objectaddress.c \
../src/backend/catalog/pg_aggregate.c \
../src/backend/catalog/pg_collation.c \
../src/backend/catalog/pg_constraint.c \
../src/backend/catalog/pg_conversion.c \
../src/backend/catalog/pg_db_role_setting.c \
../src/backend/catalog/pg_depend.c \
../src/backend/catalog/pg_enum.c \
../src/backend/catalog/pg_inherits.c \
../src/backend/catalog/pg_largeobject.c \
../src/backend/catalog/pg_namespace.c \
../src/backend/catalog/pg_operator.c \
../src/backend/catalog/pg_proc.c \
../src/backend/catalog/pg_range.c \
../src/backend/catalog/pg_shdepend.c \
../src/backend/catalog/pg_type.c \
../src/backend/catalog/storage.c \
../src/backend/catalog/toasting.c 

OBJS += \
./src/backend/catalog/aclchk.o \
./src/backend/catalog/catalog.o \
./src/backend/catalog/dependency.o \
./src/backend/catalog/heap.o \
./src/backend/catalog/index.o \
./src/backend/catalog/indexing.o \
./src/backend/catalog/namespace.o \
./src/backend/catalog/objectaccess.o \
./src/backend/catalog/objectaddress.o \
./src/backend/catalog/pg_aggregate.o \
./src/backend/catalog/pg_collation.o \
./src/backend/catalog/pg_constraint.o \
./src/backend/catalog/pg_conversion.o \
./src/backend/catalog/pg_db_role_setting.o \
./src/backend/catalog/pg_depend.o \
./src/backend/catalog/pg_enum.o \
./src/backend/catalog/pg_inherits.o \
./src/backend/catalog/pg_largeobject.o \
./src/backend/catalog/pg_namespace.o \
./src/backend/catalog/pg_operator.o \
./src/backend/catalog/pg_proc.o \
./src/backend/catalog/pg_range.o \
./src/backend/catalog/pg_shdepend.o \
./src/backend/catalog/pg_type.o \
./src/backend/catalog/storage.o \
./src/backend/catalog/toasting.o 

C_DEPS += \
./src/backend/catalog/aclchk.d \
./src/backend/catalog/catalog.d \
./src/backend/catalog/dependency.d \
./src/backend/catalog/heap.d \
./src/backend/catalog/index.d \
./src/backend/catalog/indexing.d \
./src/backend/catalog/namespace.d \
./src/backend/catalog/objectaccess.d \
./src/backend/catalog/objectaddress.d \
./src/backend/catalog/pg_aggregate.d \
./src/backend/catalog/pg_collation.d \
./src/backend/catalog/pg_constraint.d \
./src/backend/catalog/pg_conversion.d \
./src/backend/catalog/pg_db_role_setting.d \
./src/backend/catalog/pg_depend.d \
./src/backend/catalog/pg_enum.d \
./src/backend/catalog/pg_inherits.d \
./src/backend/catalog/pg_largeobject.d \
./src/backend/catalog/pg_namespace.d \
./src/backend/catalog/pg_operator.d \
./src/backend/catalog/pg_proc.d \
./src/backend/catalog/pg_range.d \
./src/backend/catalog/pg_shdepend.d \
./src/backend/catalog/pg_type.d \
./src/backend/catalog/storage.d \
./src/backend/catalog/toasting.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/catalog/%.o: ../src/backend/catalog/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


