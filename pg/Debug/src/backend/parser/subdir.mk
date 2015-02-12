################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/parser/analyze.o \
../src/backend/parser/gram.o \
../src/backend/parser/keywords.o \
../src/backend/parser/kwlookup.o \
../src/backend/parser/parse_agg.o \
../src/backend/parser/parse_clause.o \
../src/backend/parser/parse_coerce.o \
../src/backend/parser/parse_collate.o \
../src/backend/parser/parse_cte.o \
../src/backend/parser/parse_expr.o \
../src/backend/parser/parse_func.o \
../src/backend/parser/parse_node.o \
../src/backend/parser/parse_oper.o \
../src/backend/parser/parse_param.o \
../src/backend/parser/parse_relation.o \
../src/backend/parser/parse_target.o \
../src/backend/parser/parse_type.o \
../src/backend/parser/parse_utilcmd.o \
../src/backend/parser/parser.o \
../src/backend/parser/scansup.o 

C_SRCS += \
../src/backend/parser/analyze.c \
../src/backend/parser/gram.c \
../src/backend/parser/keywords.c \
../src/backend/parser/kwlookup.c \
../src/backend/parser/parse_agg.c \
../src/backend/parser/parse_clause.c \
../src/backend/parser/parse_coerce.c \
../src/backend/parser/parse_collate.c \
../src/backend/parser/parse_cte.c \
../src/backend/parser/parse_expr.c \
../src/backend/parser/parse_func.c \
../src/backend/parser/parse_node.c \
../src/backend/parser/parse_oper.c \
../src/backend/parser/parse_param.c \
../src/backend/parser/parse_relation.c \
../src/backend/parser/parse_target.c \
../src/backend/parser/parse_type.c \
../src/backend/parser/parse_utilcmd.c \
../src/backend/parser/parser.c \
../src/backend/parser/scan.c \
../src/backend/parser/scansup.c 

OBJS += \
./src/backend/parser/analyze.o \
./src/backend/parser/gram.o \
./src/backend/parser/keywords.o \
./src/backend/parser/kwlookup.o \
./src/backend/parser/parse_agg.o \
./src/backend/parser/parse_clause.o \
./src/backend/parser/parse_coerce.o \
./src/backend/parser/parse_collate.o \
./src/backend/parser/parse_cte.o \
./src/backend/parser/parse_expr.o \
./src/backend/parser/parse_func.o \
./src/backend/parser/parse_node.o \
./src/backend/parser/parse_oper.o \
./src/backend/parser/parse_param.o \
./src/backend/parser/parse_relation.o \
./src/backend/parser/parse_target.o \
./src/backend/parser/parse_type.o \
./src/backend/parser/parse_utilcmd.o \
./src/backend/parser/parser.o \
./src/backend/parser/scan.o \
./src/backend/parser/scansup.o 

C_DEPS += \
./src/backend/parser/analyze.d \
./src/backend/parser/gram.d \
./src/backend/parser/keywords.d \
./src/backend/parser/kwlookup.d \
./src/backend/parser/parse_agg.d \
./src/backend/parser/parse_clause.d \
./src/backend/parser/parse_coerce.d \
./src/backend/parser/parse_collate.d \
./src/backend/parser/parse_cte.d \
./src/backend/parser/parse_expr.d \
./src/backend/parser/parse_func.d \
./src/backend/parser/parse_node.d \
./src/backend/parser/parse_oper.d \
./src/backend/parser/parse_param.d \
./src/backend/parser/parse_relation.d \
./src/backend/parser/parse_target.d \
./src/backend/parser/parse_type.d \
./src/backend/parser/parse_utilcmd.d \
./src/backend/parser/parser.d \
./src/backend/parser/scan.d \
./src/backend/parser/scansup.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/parser/%.o: ../src/backend/parser/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


