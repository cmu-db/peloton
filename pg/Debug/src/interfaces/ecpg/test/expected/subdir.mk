################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/interfaces/ecpg/test/expected/compat_informix-charfuncs.c \
../src/interfaces/ecpg/test/expected/compat_informix-dec_test.c \
../src/interfaces/ecpg/test/expected/compat_informix-describe.c \
../src/interfaces/ecpg/test/expected/compat_informix-rfmtdate.c \
../src/interfaces/ecpg/test/expected/compat_informix-rfmtlong.c \
../src/interfaces/ecpg/test/expected/compat_informix-rnull.c \
../src/interfaces/ecpg/test/expected/compat_informix-sqlda.c \
../src/interfaces/ecpg/test/expected/compat_informix-test_informix.c \
../src/interfaces/ecpg/test/expected/compat_informix-test_informix2.c \
../src/interfaces/ecpg/test/expected/connect-test1.c \
../src/interfaces/ecpg/test/expected/connect-test2.c \
../src/interfaces/ecpg/test/expected/connect-test3.c \
../src/interfaces/ecpg/test/expected/connect-test4.c \
../src/interfaces/ecpg/test/expected/connect-test5.c \
../src/interfaces/ecpg/test/expected/pgtypeslib-dt_test.c \
../src/interfaces/ecpg/test/expected/pgtypeslib-dt_test2.c \
../src/interfaces/ecpg/test/expected/pgtypeslib-nan_test.c \
../src/interfaces/ecpg/test/expected/pgtypeslib-num_test.c \
../src/interfaces/ecpg/test/expected/pgtypeslib-num_test2.c \
../src/interfaces/ecpg/test/expected/preproc-array_of_struct.c \
../src/interfaces/ecpg/test/expected/preproc-autoprep.c \
../src/interfaces/ecpg/test/expected/preproc-comment.c \
../src/interfaces/ecpg/test/expected/preproc-cursor.c \
../src/interfaces/ecpg/test/expected/preproc-define.c \
../src/interfaces/ecpg/test/expected/preproc-describe.c \
../src/interfaces/ecpg/test/expected/preproc-init.c \
../src/interfaces/ecpg/test/expected/preproc-outofscope.c \
../src/interfaces/ecpg/test/expected/preproc-pointer_to_struct.c \
../src/interfaces/ecpg/test/expected/preproc-strings.c \
../src/interfaces/ecpg/test/expected/preproc-type.c \
../src/interfaces/ecpg/test/expected/preproc-variable.c \
../src/interfaces/ecpg/test/expected/preproc-whenever.c \
../src/interfaces/ecpg/test/expected/sql-array.c \
../src/interfaces/ecpg/test/expected/sql-binary.c \
../src/interfaces/ecpg/test/expected/sql-code100.c \
../src/interfaces/ecpg/test/expected/sql-copystdout.c \
../src/interfaces/ecpg/test/expected/sql-define.c \
../src/interfaces/ecpg/test/expected/sql-desc.c \
../src/interfaces/ecpg/test/expected/sql-describe.c \
../src/interfaces/ecpg/test/expected/sql-dynalloc.c \
../src/interfaces/ecpg/test/expected/sql-dynalloc2.c \
../src/interfaces/ecpg/test/expected/sql-dyntest.c \
../src/interfaces/ecpg/test/expected/sql-execute.c \
../src/interfaces/ecpg/test/expected/sql-fetch.c \
../src/interfaces/ecpg/test/expected/sql-func.c \
../src/interfaces/ecpg/test/expected/sql-indicators.c \
../src/interfaces/ecpg/test/expected/sql-insupd.c \
../src/interfaces/ecpg/test/expected/sql-oldexec.c \
../src/interfaces/ecpg/test/expected/sql-parser.c \
../src/interfaces/ecpg/test/expected/sql-quote.c \
../src/interfaces/ecpg/test/expected/sql-show.c \
../src/interfaces/ecpg/test/expected/sql-sqlda.c \
../src/interfaces/ecpg/test/expected/thread-alloc.c \
../src/interfaces/ecpg/test/expected/thread-descriptor.c \
../src/interfaces/ecpg/test/expected/thread-prep.c \
../src/interfaces/ecpg/test/expected/thread-thread.c \
../src/interfaces/ecpg/test/expected/thread-thread_implicit.c 

OBJS += \
./src/interfaces/ecpg/test/expected/compat_informix-charfuncs.o \
./src/interfaces/ecpg/test/expected/compat_informix-dec_test.o \
./src/interfaces/ecpg/test/expected/compat_informix-describe.o \
./src/interfaces/ecpg/test/expected/compat_informix-rfmtdate.o \
./src/interfaces/ecpg/test/expected/compat_informix-rfmtlong.o \
./src/interfaces/ecpg/test/expected/compat_informix-rnull.o \
./src/interfaces/ecpg/test/expected/compat_informix-sqlda.o \
./src/interfaces/ecpg/test/expected/compat_informix-test_informix.o \
./src/interfaces/ecpg/test/expected/compat_informix-test_informix2.o \
./src/interfaces/ecpg/test/expected/connect-test1.o \
./src/interfaces/ecpg/test/expected/connect-test2.o \
./src/interfaces/ecpg/test/expected/connect-test3.o \
./src/interfaces/ecpg/test/expected/connect-test4.o \
./src/interfaces/ecpg/test/expected/connect-test5.o \
./src/interfaces/ecpg/test/expected/pgtypeslib-dt_test.o \
./src/interfaces/ecpg/test/expected/pgtypeslib-dt_test2.o \
./src/interfaces/ecpg/test/expected/pgtypeslib-nan_test.o \
./src/interfaces/ecpg/test/expected/pgtypeslib-num_test.o \
./src/interfaces/ecpg/test/expected/pgtypeslib-num_test2.o \
./src/interfaces/ecpg/test/expected/preproc-array_of_struct.o \
./src/interfaces/ecpg/test/expected/preproc-autoprep.o \
./src/interfaces/ecpg/test/expected/preproc-comment.o \
./src/interfaces/ecpg/test/expected/preproc-cursor.o \
./src/interfaces/ecpg/test/expected/preproc-define.o \
./src/interfaces/ecpg/test/expected/preproc-describe.o \
./src/interfaces/ecpg/test/expected/preproc-init.o \
./src/interfaces/ecpg/test/expected/preproc-outofscope.o \
./src/interfaces/ecpg/test/expected/preproc-pointer_to_struct.o \
./src/interfaces/ecpg/test/expected/preproc-strings.o \
./src/interfaces/ecpg/test/expected/preproc-type.o \
./src/interfaces/ecpg/test/expected/preproc-variable.o \
./src/interfaces/ecpg/test/expected/preproc-whenever.o \
./src/interfaces/ecpg/test/expected/sql-array.o \
./src/interfaces/ecpg/test/expected/sql-binary.o \
./src/interfaces/ecpg/test/expected/sql-code100.o \
./src/interfaces/ecpg/test/expected/sql-copystdout.o \
./src/interfaces/ecpg/test/expected/sql-define.o \
./src/interfaces/ecpg/test/expected/sql-desc.o \
./src/interfaces/ecpg/test/expected/sql-describe.o \
./src/interfaces/ecpg/test/expected/sql-dynalloc.o \
./src/interfaces/ecpg/test/expected/sql-dynalloc2.o \
./src/interfaces/ecpg/test/expected/sql-dyntest.o \
./src/interfaces/ecpg/test/expected/sql-execute.o \
./src/interfaces/ecpg/test/expected/sql-fetch.o \
./src/interfaces/ecpg/test/expected/sql-func.o \
./src/interfaces/ecpg/test/expected/sql-indicators.o \
./src/interfaces/ecpg/test/expected/sql-insupd.o \
./src/interfaces/ecpg/test/expected/sql-oldexec.o \
./src/interfaces/ecpg/test/expected/sql-parser.o \
./src/interfaces/ecpg/test/expected/sql-quote.o \
./src/interfaces/ecpg/test/expected/sql-show.o \
./src/interfaces/ecpg/test/expected/sql-sqlda.o \
./src/interfaces/ecpg/test/expected/thread-alloc.o \
./src/interfaces/ecpg/test/expected/thread-descriptor.o \
./src/interfaces/ecpg/test/expected/thread-prep.o \
./src/interfaces/ecpg/test/expected/thread-thread.o \
./src/interfaces/ecpg/test/expected/thread-thread_implicit.o 

C_DEPS += \
./src/interfaces/ecpg/test/expected/compat_informix-charfuncs.d \
./src/interfaces/ecpg/test/expected/compat_informix-dec_test.d \
./src/interfaces/ecpg/test/expected/compat_informix-describe.d \
./src/interfaces/ecpg/test/expected/compat_informix-rfmtdate.d \
./src/interfaces/ecpg/test/expected/compat_informix-rfmtlong.d \
./src/interfaces/ecpg/test/expected/compat_informix-rnull.d \
./src/interfaces/ecpg/test/expected/compat_informix-sqlda.d \
./src/interfaces/ecpg/test/expected/compat_informix-test_informix.d \
./src/interfaces/ecpg/test/expected/compat_informix-test_informix2.d \
./src/interfaces/ecpg/test/expected/connect-test1.d \
./src/interfaces/ecpg/test/expected/connect-test2.d \
./src/interfaces/ecpg/test/expected/connect-test3.d \
./src/interfaces/ecpg/test/expected/connect-test4.d \
./src/interfaces/ecpg/test/expected/connect-test5.d \
./src/interfaces/ecpg/test/expected/pgtypeslib-dt_test.d \
./src/interfaces/ecpg/test/expected/pgtypeslib-dt_test2.d \
./src/interfaces/ecpg/test/expected/pgtypeslib-nan_test.d \
./src/interfaces/ecpg/test/expected/pgtypeslib-num_test.d \
./src/interfaces/ecpg/test/expected/pgtypeslib-num_test2.d \
./src/interfaces/ecpg/test/expected/preproc-array_of_struct.d \
./src/interfaces/ecpg/test/expected/preproc-autoprep.d \
./src/interfaces/ecpg/test/expected/preproc-comment.d \
./src/interfaces/ecpg/test/expected/preproc-cursor.d \
./src/interfaces/ecpg/test/expected/preproc-define.d \
./src/interfaces/ecpg/test/expected/preproc-describe.d \
./src/interfaces/ecpg/test/expected/preproc-init.d \
./src/interfaces/ecpg/test/expected/preproc-outofscope.d \
./src/interfaces/ecpg/test/expected/preproc-pointer_to_struct.d \
./src/interfaces/ecpg/test/expected/preproc-strings.d \
./src/interfaces/ecpg/test/expected/preproc-type.d \
./src/interfaces/ecpg/test/expected/preproc-variable.d \
./src/interfaces/ecpg/test/expected/preproc-whenever.d \
./src/interfaces/ecpg/test/expected/sql-array.d \
./src/interfaces/ecpg/test/expected/sql-binary.d \
./src/interfaces/ecpg/test/expected/sql-code100.d \
./src/interfaces/ecpg/test/expected/sql-copystdout.d \
./src/interfaces/ecpg/test/expected/sql-define.d \
./src/interfaces/ecpg/test/expected/sql-desc.d \
./src/interfaces/ecpg/test/expected/sql-describe.d \
./src/interfaces/ecpg/test/expected/sql-dynalloc.d \
./src/interfaces/ecpg/test/expected/sql-dynalloc2.d \
./src/interfaces/ecpg/test/expected/sql-dyntest.d \
./src/interfaces/ecpg/test/expected/sql-execute.d \
./src/interfaces/ecpg/test/expected/sql-fetch.d \
./src/interfaces/ecpg/test/expected/sql-func.d \
./src/interfaces/ecpg/test/expected/sql-indicators.d \
./src/interfaces/ecpg/test/expected/sql-insupd.d \
./src/interfaces/ecpg/test/expected/sql-oldexec.d \
./src/interfaces/ecpg/test/expected/sql-parser.d \
./src/interfaces/ecpg/test/expected/sql-quote.d \
./src/interfaces/ecpg/test/expected/sql-show.d \
./src/interfaces/ecpg/test/expected/sql-sqlda.d \
./src/interfaces/ecpg/test/expected/thread-alloc.d \
./src/interfaces/ecpg/test/expected/thread-descriptor.d \
./src/interfaces/ecpg/test/expected/thread-prep.d \
./src/interfaces/ecpg/test/expected/thread-thread.d \
./src/interfaces/ecpg/test/expected/thread-thread_implicit.d 


# Each subdirectory must supply rules for building sources it contributes
src/interfaces/ecpg/test/expected/%.o: ../src/interfaces/ecpg/test/expected/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


