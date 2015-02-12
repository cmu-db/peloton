################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/tsearch/dict.o \
../src/backend/tsearch/dict_ispell.o \
../src/backend/tsearch/dict_simple.o \
../src/backend/tsearch/dict_synonym.o \
../src/backend/tsearch/dict_thesaurus.o \
../src/backend/tsearch/regis.o \
../src/backend/tsearch/spell.o \
../src/backend/tsearch/to_tsany.o \
../src/backend/tsearch/ts_locale.o \
../src/backend/tsearch/ts_parse.o \
../src/backend/tsearch/ts_selfuncs.o \
../src/backend/tsearch/ts_typanalyze.o \
../src/backend/tsearch/ts_utils.o \
../src/backend/tsearch/wparser.o \
../src/backend/tsearch/wparser_def.o 

C_SRCS += \
../src/backend/tsearch/dict.c \
../src/backend/tsearch/dict_ispell.c \
../src/backend/tsearch/dict_simple.c \
../src/backend/tsearch/dict_synonym.c \
../src/backend/tsearch/dict_thesaurus.c \
../src/backend/tsearch/regis.c \
../src/backend/tsearch/spell.c \
../src/backend/tsearch/to_tsany.c \
../src/backend/tsearch/ts_locale.c \
../src/backend/tsearch/ts_parse.c \
../src/backend/tsearch/ts_selfuncs.c \
../src/backend/tsearch/ts_typanalyze.c \
../src/backend/tsearch/ts_utils.c \
../src/backend/tsearch/wparser.c \
../src/backend/tsearch/wparser_def.c 

OBJS += \
./src/backend/tsearch/dict.o \
./src/backend/tsearch/dict_ispell.o \
./src/backend/tsearch/dict_simple.o \
./src/backend/tsearch/dict_synonym.o \
./src/backend/tsearch/dict_thesaurus.o \
./src/backend/tsearch/regis.o \
./src/backend/tsearch/spell.o \
./src/backend/tsearch/to_tsany.o \
./src/backend/tsearch/ts_locale.o \
./src/backend/tsearch/ts_parse.o \
./src/backend/tsearch/ts_selfuncs.o \
./src/backend/tsearch/ts_typanalyze.o \
./src/backend/tsearch/ts_utils.o \
./src/backend/tsearch/wparser.o \
./src/backend/tsearch/wparser_def.o 

C_DEPS += \
./src/backend/tsearch/dict.d \
./src/backend/tsearch/dict_ispell.d \
./src/backend/tsearch/dict_simple.d \
./src/backend/tsearch/dict_synonym.d \
./src/backend/tsearch/dict_thesaurus.d \
./src/backend/tsearch/regis.d \
./src/backend/tsearch/spell.d \
./src/backend/tsearch/to_tsany.d \
./src/backend/tsearch/ts_locale.d \
./src/backend/tsearch/ts_parse.d \
./src/backend/tsearch/ts_selfuncs.d \
./src/backend/tsearch/ts_typanalyze.d \
./src/backend/tsearch/ts_utils.d \
./src/backend/tsearch/wparser.d \
./src/backend/tsearch/wparser_def.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/tsearch/%.o: ../src/backend/tsearch/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


