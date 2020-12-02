#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <Esent.h>

#include "sqlite3esent.h"

/* Following function is copy-pasted from SQLite sources */

/*
** This function converts an SQL quoted string into an unquoted string
** and returns a pointer to a buffer allocated using sqlite3_malloc()
** containing the result. The caller should eventually free this buffer
** using sqlite3_free.
**
** Examples:
**
**     "abc"   becomes   abc
**     'xyz'   becomes   xyz
**     [pqr]   becomes   pqr
**     `mno`   becomes   mno
*/
char* esentDequote(const char* zIn) {
	sqlite3_int64 nIn;              /* Size of input string, in bytes */
	char* zOut;                     /* Output (dequoted) string */

	nIn = strlen(zIn);
	zOut = sqlite3_malloc64(nIn + 1);
	if (zOut) {
		char q = zIn[0];              /* Quote character (if any ) */

		if (q != '[' && q != '\'' && q != '"' && q != '`') {
			memcpy(zOut, zIn, (size_t)(nIn + 1));
		}
		else {
			int iOut = 0;               /* Index of next byte to write to output */
			int iIn;                    /* Index of next byte to read from input */

			if (q == '[') q = ']';
			for (iIn = 1; iIn < nIn; iIn++) {
				if (zIn[iIn] == q) iIn++;
				zOut[iOut++] = zIn[iIn];
			}
		}
		assert((int)strlen(zOut) <= nIn);
	}
	return zOut;
}
