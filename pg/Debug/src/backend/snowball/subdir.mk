################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../src/backend/snowball/api.o \
../src/backend/snowball/dict_snowball.o \
../src/backend/snowball/stem_ISO_8859_1_danish.o \
../src/backend/snowball/stem_ISO_8859_1_dutch.o \
../src/backend/snowball/stem_ISO_8859_1_english.o \
../src/backend/snowball/stem_ISO_8859_1_finnish.o \
../src/backend/snowball/stem_ISO_8859_1_french.o \
../src/backend/snowball/stem_ISO_8859_1_german.o \
../src/backend/snowball/stem_ISO_8859_1_hungarian.o \
../src/backend/snowball/stem_ISO_8859_1_italian.o \
../src/backend/snowball/stem_ISO_8859_1_norwegian.o \
../src/backend/snowball/stem_ISO_8859_1_porter.o \
../src/backend/snowball/stem_ISO_8859_1_portuguese.o \
../src/backend/snowball/stem_ISO_8859_1_spanish.o \
../src/backend/snowball/stem_ISO_8859_1_swedish.o \
../src/backend/snowball/stem_ISO_8859_2_romanian.o \
../src/backend/snowball/stem_KOI8_R_russian.o \
../src/backend/snowball/stem_UTF_8_danish.o \
../src/backend/snowball/stem_UTF_8_dutch.o \
../src/backend/snowball/stem_UTF_8_english.o \
../src/backend/snowball/stem_UTF_8_finnish.o \
../src/backend/snowball/stem_UTF_8_french.o \
../src/backend/snowball/stem_UTF_8_german.o \
../src/backend/snowball/stem_UTF_8_hungarian.o \
../src/backend/snowball/stem_UTF_8_italian.o \
../src/backend/snowball/stem_UTF_8_norwegian.o \
../src/backend/snowball/stem_UTF_8_porter.o \
../src/backend/snowball/stem_UTF_8_portuguese.o \
../src/backend/snowball/stem_UTF_8_romanian.o \
../src/backend/snowball/stem_UTF_8_russian.o \
../src/backend/snowball/stem_UTF_8_spanish.o \
../src/backend/snowball/stem_UTF_8_swedish.o \
../src/backend/snowball/stem_UTF_8_turkish.o \
../src/backend/snowball/utilities.o 

C_SRCS += \
../src/backend/snowball/dict_snowball.c 

OBJS += \
./src/backend/snowball/dict_snowball.o 

C_DEPS += \
./src/backend/snowball/dict_snowball.d 


# Each subdirectory must supply rules for building sources it contributes
src/backend/snowball/%.o: ../src/backend/snowball/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


