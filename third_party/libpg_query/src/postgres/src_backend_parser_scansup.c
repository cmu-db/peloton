/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - truncate_identifier
 * - downcase_truncate_identifier
 * - scanner_isspace
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * scansup.c
 *	  support routines for the lex/flex scanner, used by both the normal
 * backend as well as the bootstrap backend
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/scansup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "parser/scansup.h"
#include "mb/pg_wchar.h"


/* ----------------
 *		scanstr
 *
 * if the string passed in has escaped codes, map the escape codes to actual
 * chars
 *
 * the string returned is palloc'd and should eventually be pfree'd by the
 * caller!
 * ----------------
 */




/*
 * downcase_truncate_identifier() --- do appropriate downcasing and
 * truncation of an unquoted identifier.  Optionally warn of truncation.
 *
 * Returns a palloc'd string containing the adjusted identifier.
 *
 * Note: in some usages the passed string is not null-terminated.
 *
 * Note: the API of this function is designed to allow for downcasing
 * transformations that increase the string length, but we don't yet
 * support that.  If you want to implement it, you'll need to fix
 * SplitIdentifierString() in utils/adt/varlena.c.
 */
char *
downcase_truncate_identifier(const char *ident, int len, bool warn)
{
	char	   *result;
	int			i;
	bool		enc_is_single_byte;

	result = palloc(len + 1);
	enc_is_single_byte = pg_database_encoding_max_length() == 1;

	/*
	 * SQL99 specifies Unicode-aware case normalization, which we don't yet
	 * have the infrastructure for.  Instead we use tolower() to provide a
	 * locale-aware translation.  However, there are some locales where this
	 * is not right either (eg, Turkish may do strange things with 'i' and
	 * 'I').  Our current compromise is to use tolower() for characters with
	 * the high bit set, as long as they aren't part of a multi-byte
	 * character, and use an ASCII-only downcasing for 7-bit characters.
	 */
	for (i = 0; i < len; i++)
	{
		unsigned char ch = (unsigned char) ident[i];

		if (ch >= 'A' && ch <= 'Z')
			ch += 'a' - 'A';
		else if (enc_is_single_byte && IS_HIGHBIT_SET(ch) && isupper(ch))
			ch = tolower(ch);
		result[i] = (char) ch;
	}
	result[i] = '\0';

	if (i >= NAMEDATALEN)
		truncate_identifier(result, i, warn);

	return result;
}

/*
 * truncate_identifier() --- truncate an identifier to NAMEDATALEN-1 bytes.
 *
 * The given string is modified in-place, if necessary.  A warning is
 * issued if requested.
 *
 * We require the caller to pass in the string length since this saves a
 * strlen() call in some common usages.
 */
void
truncate_identifier(char *ident, int len, bool warn)
{
	if (len >= NAMEDATALEN)
	{
		len = pg_mbcliplen(ident, len, NAMEDATALEN - 1);
		if (warn)
		{
			/*
			 * We avoid using %.*s here because it can misbehave if the data
			 * is not valid in what libc thinks is the prevailing encoding.
			 */
			char		buf[NAMEDATALEN];

			memcpy(buf, ident, len);
			buf[len] = '\0';
			ereport(NOTICE,
					(errcode(ERRCODE_NAME_TOO_LONG),
					 errmsg("identifier \"%s\" will be truncated to \"%s\"",
							ident, buf)));
		}
		ident[len] = '\0';
	}
}

/*
 * scanner_isspace() --- return TRUE if flex scanner considers char whitespace
 *
 * This should be used instead of the potentially locale-dependent isspace()
 * function when it's important to match the lexer's behavior.
 *
 * In principle we might need similar functions for isalnum etc, but for the
 * moment only isspace seems needed.
 */
bool
scanner_isspace(char ch)
{
	/* This must match scan.l's list of {space} characters */
	if (ch == ' ' ||
		ch == '\t' ||
		ch == '\n' ||
		ch == '\r' ||
		ch == '\f')
		return true;
	return false;
}
