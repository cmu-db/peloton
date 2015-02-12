################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/regex/regcomp.o \
../src/backend/regex/regerror.o \
../src/backend/regex/regexec.o \
../src/backend/regex/regexport.o \
../src/backend/regex/regfree.o \
../src/backend/regex/regprefix.o 

C_SRCS += \
../src/backend/regex/regc_color.c \
../src/backend/regex/regc_cvec.c \
../src/backend/regex/regc_lex.c \
../src/backend/regex/regc_locale.c \
../src/backend/regex/regc_nfa.c \
../src/backend/regex/regc_pg_locale.c \
../src/backend/regex/regcomp.c \
../src/backend/regex/rege_dfa.c \
../src/backend/regex/regerror.c \
../src/backend/regex/regexec.c \
../src/backend/regex/regexport.c \
../src/backend/regex/regfree.c \
../src/backend/regex/regprefix.c 

OBJS += \
./src/backend/regex/regc_color.o \
./src/backend/regex/regc_cvec.o \
./src/backend/regex/regc_lex.o \
./src/backend/regex/regc_locale.o \
./src/backend/regex/regc_nfa.o \
./src/backend/regex/regc_pg_locale.o \
./src/backend/regex/regcomp.o \
./src/backend/regex/rege_dfa.o \
./src/backend/regex/regerror.o \
./src/backend/regex/regexec.o \
./src/backend/regex/regexport.o \
./src/backend/regex/regfree.o \
./src/backend/regex/regprefix.o 

C_DEPS += \
./src/backend/regex/regc_color.d \
./src/backend/regex/regc_cvec.d \
./src/backend/regex/regc_lex.d \
./src/backend/regex/regc_locale.d \
./src/backend/regex/regc_nfa.d \
./src/backend/regex/regc_pg_locale.d \
./src/backend/regex/regcomp.d \
./src/backend/regex/rege_dfa.d \
./src/backend/regex/regerror.d \
./src/backend/regex/regexec.d \
./src/backend/regex/regexport.d \
./src/backend/regex/regfree.d \
./src/backend/regex/regprefix.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/regex/%.o: ../src/backend/regex/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


