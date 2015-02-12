################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../contrib/pgcrypto/blf.c \
../contrib/pgcrypto/crypt-blowfish.c \
../contrib/pgcrypto/crypt-des.c \
../contrib/pgcrypto/crypt-gensalt.c \
../contrib/pgcrypto/crypt-md5.c \
../contrib/pgcrypto/fortuna.c \
../contrib/pgcrypto/imath.c \
../contrib/pgcrypto/internal-sha2.c \
../contrib/pgcrypto/internal.c \
../contrib/pgcrypto/mbuf.c \
../contrib/pgcrypto/md5.c \
../contrib/pgcrypto/openssl.c \
../contrib/pgcrypto/pgcrypto.c \
../contrib/pgcrypto/pgp-armor.c \
../contrib/pgcrypto/pgp-cfb.c \
../contrib/pgcrypto/pgp-compress.c \
../contrib/pgcrypto/pgp-decrypt.c \
../contrib/pgcrypto/pgp-encrypt.c \
../contrib/pgcrypto/pgp-info.c \
../contrib/pgcrypto/pgp-mpi-internal.c \
../contrib/pgcrypto/pgp-mpi-openssl.c \
../contrib/pgcrypto/pgp-mpi.c \
../contrib/pgcrypto/pgp-pgsql.c \
../contrib/pgcrypto/pgp-pubdec.c \
../contrib/pgcrypto/pgp-pubenc.c \
../contrib/pgcrypto/pgp-pubkey.c \
../contrib/pgcrypto/pgp-s2k.c \
../contrib/pgcrypto/pgp.c \
../contrib/pgcrypto/px-crypt.c \
../contrib/pgcrypto/px-hmac.c \
../contrib/pgcrypto/px.c \
../contrib/pgcrypto/random.c \
../contrib/pgcrypto/rijndael.c \
../contrib/pgcrypto/sha1.c \
../contrib/pgcrypto/sha2.c 

OBJS += \
./contrib/pgcrypto/blf.o \
./contrib/pgcrypto/crypt-blowfish.o \
./contrib/pgcrypto/crypt-des.o \
./contrib/pgcrypto/crypt-gensalt.o \
./contrib/pgcrypto/crypt-md5.o \
./contrib/pgcrypto/fortuna.o \
./contrib/pgcrypto/imath.o \
./contrib/pgcrypto/internal-sha2.o \
./contrib/pgcrypto/internal.o \
./contrib/pgcrypto/mbuf.o \
./contrib/pgcrypto/md5.o \
./contrib/pgcrypto/openssl.o \
./contrib/pgcrypto/pgcrypto.o \
./contrib/pgcrypto/pgp-armor.o \
./contrib/pgcrypto/pgp-cfb.o \
./contrib/pgcrypto/pgp-compress.o \
./contrib/pgcrypto/pgp-decrypt.o \
./contrib/pgcrypto/pgp-encrypt.o \
./contrib/pgcrypto/pgp-info.o \
./contrib/pgcrypto/pgp-mpi-internal.o \
./contrib/pgcrypto/pgp-mpi-openssl.o \
./contrib/pgcrypto/pgp-mpi.o \
./contrib/pgcrypto/pgp-pgsql.o \
./contrib/pgcrypto/pgp-pubdec.o \
./contrib/pgcrypto/pgp-pubenc.o \
./contrib/pgcrypto/pgp-pubkey.o \
./contrib/pgcrypto/pgp-s2k.o \
./contrib/pgcrypto/pgp.o \
./contrib/pgcrypto/px-crypt.o \
./contrib/pgcrypto/px-hmac.o \
./contrib/pgcrypto/px.o \
./contrib/pgcrypto/random.o \
./contrib/pgcrypto/rijndael.o \
./contrib/pgcrypto/sha1.o \
./contrib/pgcrypto/sha2.o 

C_DEPS += \
./contrib/pgcrypto/blf.d \
./contrib/pgcrypto/crypt-blowfish.d \
./contrib/pgcrypto/crypt-des.d \
./contrib/pgcrypto/crypt-gensalt.d \
./contrib/pgcrypto/crypt-md5.d \
./contrib/pgcrypto/fortuna.d \
./contrib/pgcrypto/imath.d \
./contrib/pgcrypto/internal-sha2.d \
./contrib/pgcrypto/internal.d \
./contrib/pgcrypto/mbuf.d \
./contrib/pgcrypto/md5.d \
./contrib/pgcrypto/openssl.d \
./contrib/pgcrypto/pgcrypto.d \
./contrib/pgcrypto/pgp-armor.d \
./contrib/pgcrypto/pgp-cfb.d \
./contrib/pgcrypto/pgp-compress.d \
./contrib/pgcrypto/pgp-decrypt.d \
./contrib/pgcrypto/pgp-encrypt.d \
./contrib/pgcrypto/pgp-info.d \
./contrib/pgcrypto/pgp-mpi-internal.d \
./contrib/pgcrypto/pgp-mpi-openssl.d \
./contrib/pgcrypto/pgp-mpi.d \
./contrib/pgcrypto/pgp-pgsql.d \
./contrib/pgcrypto/pgp-pubdec.d \
./contrib/pgcrypto/pgp-pubenc.d \
./contrib/pgcrypto/pgp-pubkey.d \
./contrib/pgcrypto/pgp-s2k.d \
./contrib/pgcrypto/pgp.d \
./contrib/pgcrypto/px-crypt.d \
./contrib/pgcrypto/px-hmac.d \
./contrib/pgcrypto/px.d \
./contrib/pgcrypto/random.d \
./contrib/pgcrypto/rijndael.d \
./contrib/pgcrypto/sha1.d \
./contrib/pgcrypto/sha2.d 


# Each subdirectory must supply rules for building sources it contributes
contrib/pgcrypto/%.o: ../contrib/pgcrypto/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


