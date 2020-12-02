#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <Esent.h>

#include "sqlite3esent.h"

typedef struct EsentVtab EsentVtab;
struct EsentVtab {
	sqlite3_vtab base;
	JET_INSTANCE jinstance; // probably should be stored at module level
	JET_SESID jses; // same
	JET_DBID jdb; // same
	char* tabName;
	// TODO wrap into struct
	JET_COLUMNID* colIds;
	JET_COLTYP* colTyps;
};

typedef struct EsentVtabCursor EsentVtabCursor;
struct EsentVtabCursor {
	sqlite3_vtab_cursor base;
	JET_TABLEID jtab;
	JET_ERR moveErr;
};

/*
TODO add function esent_open(dbfile) that attaches all tables as virtual
TODO add pseudo-vfs esent that wraps memdb and then in sqlite3_auto_extension calls esent_open()
*/

static int jetErrorToSqlite(JET_ERR jrc, char** pzErr, char* errSuffix) {
	JET_API_PTR _jrc = jrc;
	JET_API_PTR* pjrc = &_jrc;
	char jerrStr[1024];
	JET_ERR jrc2 = JetGetSystemParameterA(0, 0, JET_paramErrorToString, pjrc, jerrStr, sizeof(jerrStr));
	if (errSuffix == 0) {
		errSuffix = "";
	}
	if (jrc2 == JET_errSuccess) {
		*pzErr = sqlite3_mprintf("%scode %d: %s", errSuffix, jrc, jerrStr);
	}
	else {
		*pzErr = sqlite3_mprintf("%scode %d", jrc);
	}
	return jrc2;
}

typedef struct coltypMapping {
	unsigned int jetTyp;
	const char* jetName;
	const char* sqlType;
} coltypMapping;

static coltypMapping coltypMap[JET_coltypMax] = {
#define _(nam, sql) [JET_coltyp##nam] = { .jetTyp = JET_coltyp##nam, .jetName = #nam, .sqlType = #sql }
	_(Nil, NULL),
	_(Bit, BOOLEAN),
	_(UnsignedByte, SMALLINT),
	_(Short, SMALLINT),
	_(Long, INT),
	_(Currency, BIGINT),
	_(IEEESingle, FLOAT),
	_(IEEEDouble, DOUBLE),
	_(DateTime, DATETIME),
	_(Binary, BLOB),
	_(Text, TEXT),
	_(LongBinary, BLOB),
	_(SLV, UNKNOWN),
	_(UnsignedLong, INT),
	_(LongLong, BIGINT),
	_(GUID, BLOB),
	_(UnsignedShort, SMALLINT),
	_(UnsignedLongLong, BIGINT),
#undef _
};

static coltypMapping badcoltypInfo = { -1, 0 };

static coltypMapping coltypInfo(JET_COLTYP typ) {
	if (typ < _countof(coltypMap))
		return coltypMap[typ];
	return badcoltypInfo;
}

static const char* coltypStr(JET_COLTYP typ) {
	if (typ < _countof(coltypMap))
		return coltypMap[typ].jetName;
	return "?";
}

static int esentVtabCreate(
	sqlite3* db,
	void* pAux,
	int argc, const char* const* argv,
	sqlite3_vtab** ppVtab,
	char** pzErr
) {
	JET_ERR jrc = JET_errSuccess;
	int rc = SQLITE_OK;

	// argv: module_name db_name table_name edb_path edb_table
	if (argc != 5) {
		*pzErr = sqlite3_mprintf("expected 2 arguments: esentvtab('edb_path',edb_table)");
		return SQLITE_ERROR;
	}

	const char* mod_name = argv[0];
	const char* db_name = argv[1];
	const char* tab_name = argv[2];
	char* edb_path = esentDequote(argv[3]);
	const char* edb_table = argv[4];

	EsentVtab* pTab = sqlite3_malloc(sizeof(EsentVtab));
	memset(pTab, 0, sizeof(EsentVtab));

	pTab->tabName = _strdup(edb_table);

	// Jet instance
	// TODO should be global to module
	// TODO reduce amount of eventlogs (or is this only in debug?)

	jrc = JetCreateInstanceA(&pTab->jinstance, "Unique instance for esentVtab");
	if (jrc != JET_errSuccess) {
		pTab->jinstance = 0;
		jetErrorToSqlite(jrc, pzErr, "Failed to create JET instance: ");
		rc = SQLITE_ERROR;
		goto error;
	}

	// Jet system parameters

	// Jet init

	jrc = JetInit3A(&pTab->jinstance, 0, 0);
	if (jrc != JET_errSuccess) {
		jetErrorToSqlite(jrc, pzErr, "Failed to initialize JET: ");
		rc = SQLITE_ERROR;
		goto error;
	}

	// Jet session

	jrc = JetBeginSessionA(pTab->jinstance, &pTab->jses, 0, 0);
	if (jrc != JET_errSuccess) {
		pTab->jses = 0;
		jetErrorToSqlite(jrc, pzErr, "Failed to begin JET session: ");
		rc = SQLITE_ERROR;
		goto error;
	}

	// Jet attach

	int dbMaxSize = 1024 * 1024; // 1M * 4kb pages = 4GB
	jrc = JetAttachDatabase2A(pTab->jses, edb_path, dbMaxSize, JET_bitDbReadOnly); // TODO for now read-only
	if (jrc != JET_errSuccess) {
		jetErrorToSqlite(jrc, pzErr, "Failed to attach JET database: ");
		rc = SQLITE_ERROR;
		goto error;
	}

	jrc = JetOpenDatabaseA(pTab->jses, edb_path, NULL, &pTab->jdb, JET_bitDbReadOnly);
	if (jrc != JET_errSuccess) {
		pTab->jdb = 0;
		jetErrorToSqlite(jrc, pzErr, "Failed to open JET database: ");
		rc = SQLITE_ERROR;
		goto error;
	}


	// Begin building schema
	sqlite3_str* sb = sqlite3_str_new(db);
	sqlite3_str_appendall(sb, "CREATE TABLE v(");

	// Read columns
	{
		JET_COLUMNLIST cols;
		jrc = JetGetColumnInfoA(pTab->jses, pTab->jdb, edb_table, 0, &cols, sizeof(cols), JET_ColInfoListSortColumnid);
		if (jrc != JET_errSuccess) {
			jetErrorToSqlite(jrc, pzErr, "Failed to get column list: ");
			rc = SQLITE_ERROR;
			goto error;
		}

		pTab->colIds = sqlite3_malloc(sizeof(JET_COLUMNID) * cols.cRecord);
		pTab->colTyps = sqlite3_malloc(sizeof(JET_COLTYP) * cols.cRecord); // TODO free

		for (unsigned int nCol = 0; nCol < cols.cRecord; nCol++) {
			char colName[JET_cbNameMost + 1] = { 0 };
			JET_COLTYP colTyp = 0;
			JET_COLUMNID colId = 0;
			unsigned long cbMax = 0;
			JET_RETRIEVECOLUMN retrieve_column[] = {
				[0] = {.columnid = cols.columnidcolumnname, .pvData = colName, .cbData = sizeof(colName), .itagSequence = 1 },
				[1] = {.columnid = cols.columnidcoltyp, .pvData = &colTyp, .cbData = sizeof(colTyp), .itagSequence = 1 },
				[2] = {.columnid = cols.columnidcbMax, .pvData = &cbMax, .cbData = sizeof(cbMax), .itagSequence = 1 },
				[3] = {.columnid = cols.columnidcolumnid, .pvData = &colId, .cbData = sizeof(colId), .itagSequence = 1}
			};
			jrc = JetRetrieveColumns(pTab->jses, cols.tableid, retrieve_column, _countof(retrieve_column));

			// TODO how to handle multi-value columns?

			pTab->colIds[nCol] = colId;
			pTab->colTyps[nCol] = colTyp;

			const char* sqlType = coltypMap[colTyp].sqlType;

			sqlite3_str_appendf(sb, "%s %s", colName, sqlType);

			if (nCol < cols.cRecord - 1) {
				sqlite3_str_appendall(sb, ", ");
				JetMove(pTab->jses, cols.tableid, JET_MoveNext, 0);
			}
		}

		JetCloseTable(pTab->jses, cols.tableid);
	}

	// Read indices
	{
		JET_INDEXLIST idxs;
		jrc = JetGetIndexInfoA(pTab->jses, pTab->jdb, edb_table, 0, &idxs, sizeof(idxs), JET_IdxInfo);
		for (unsigned int nIdx = 0; nIdx < idxs.cRecord; nIdx++) {
			char idxName[JET_cbNameMost + 1] = { 0 };
			char colName[JET_cbNameMost + 1] = { 0 };
			JET_COLTYP colTyp = 0;
			unsigned long iColumn = 0;
			unsigned long cbMax = 0;
			JET_RETRIEVECOLUMN retrieve_column[] = {
				[0] = {.columnid = idxs.columnidindexname, .pvData = idxName, .cbData = sizeof(idxName), .itagSequence = 1 },
				[1] = {.columnid = idxs.columnidcolumnname, .pvData = colName, .cbData = sizeof(colName), .itagSequence = 1 },
				[2] = {.columnid = idxs.columnidcoltyp, .pvData = &colTyp, .cbData = sizeof(colTyp), .itagSequence = 1 },
				[2] = {.columnid = idxs.columnidiColumn, .pvData = &iColumn, .cbData = sizeof(iColumn), .itagSequence = 1 },
			};
			jrc = JetRetrieveColumns(pTab->jses, idxs.tableid, retrieve_column, _countof(retrieve_column));

			if (nIdx < idxs.cRecord - 1)
				JetMove(pTab->jses, idxs.tableid, JET_MoveNext, 0);
		}

		JetCloseTable(pTab->jses, idxs.tableid);
	}

	sqlite3_str_appendall(sb, ")");
	char* schema = sqlite3_str_finish(sb);

	rc = sqlite3_declare_vtab(db, schema);
	sqlite3_free(schema);

	*ppVtab = (sqlite3_vtab*)pTab;

error:
	if (rc != SQLITE_OK) {
		if (pTab->jinstance != 0) JetTerm2(pTab->jinstance, 0);

		if (pTab->tabName) {
			free(pTab->tabName);
			pTab->tabName = 0;
		}
		if (pTab->colIds) {
			sqlite3_free(pTab->colIds);
			pTab->colIds = 0;
		}
		sqlite3_free(pTab);
	}

	sqlite3_free(edb_path);

	return rc;
}

static int esentVtabConnect(
	sqlite3* db,
	void* pAux,
	int argc, const char* const* argv,
	sqlite3_vtab** ppVtab,
	char** pzErr
) {
	int rc = SQLITE_NOTFOUND;
	return rc;
}

static int esentVtabDisconnect(sqlite3_vtab* pVtab) {
	EsentVtab* p = (EsentVtab*)pVtab;
	JetTerm2(p->jinstance, 0);
	if (p->tabName) {
		free(p->tabName);
		p->tabName = 0;
	}
	return SQLITE_OK;
}

static int esentVtabDestroy(sqlite3_vtab* pVtab) {
	EsentVtab* p = (EsentVtab*)pVtab;
	JetTerm2(p->jinstance, 0);
	if (p->tabName) {
		free(p->tabName);
		p->tabName = 0;
	}
	return SQLITE_OK;
}

static int esentVtabBestIndex(
	sqlite3_vtab* tab,
	sqlite3_index_info* pIdxInfo
) {
	pIdxInfo->estimatedCost = 1000000;
	// TODO implement index support
	return SQLITE_OK;
}

static int esentVtabOpen(sqlite3_vtab* p, sqlite3_vtab_cursor** ppCursor) {
	JET_ERR jrc = JET_errSuccess;
	int rc = SQLITE_OK;

	EsentVtab* pTab = (EsentVtab*)p;
	EsentVtabCursor* pCur = (EsentVtabCursor*)sqlite3_malloc(sizeof(EsentVtabCursor));

	jrc = JetOpenTableA(pTab->jses, pTab->jdb, pTab->tabName, NULL, 0, 0, &pCur->jtab);
	if (jrc != JET_errSuccess) {
		pCur->jtab = 0;
		jetErrorToSqlite(jrc, &pCur->base.pVtab->zErrMsg, "Failed to open JET table: ");
		rc = SQLITE_ERROR;
		goto error;
	}

	*ppCursor = &pCur->base;

error:

	return SQLITE_OK;
}

static int esentVtabClose(sqlite3_vtab_cursor* pVtabCursor) {
	EsentVtabCursor* pCur = (EsentVtabCursor*)pVtabCursor;
	EsentVtab* pTab = (EsentVtab*)pVtabCursor->pVtab;
	JetCloseTable(pTab->jinstance, pCur->jtab);
	sqlite3_free(pVtabCursor);
	return SQLITE_OK;
}

static int esentVtabFilter(
	sqlite3_vtab_cursor* pVtabCursor,
	int idxNum, const char* idxStr,
	int argc, sqlite3_value** argv
) {
	EsentVtabCursor* pCur = (EsentVtabCursor*)pVtabCursor;
	EsentVtab* pTab = (EsentVtab*)pVtabCursor->pVtab;
	JET_ERR jrc;

	// for now just rewind
	jrc = JetMove(pTab->jses, pCur->jtab, JET_MoveFirst, 0);
	pCur->moveErr = jrc;
	if (jrc != JET_errSuccess && jrc != JET_errNoCurrentRecord) {
		return SQLITE_ERROR;
	}

	return SQLITE_OK;
}

static int esentVtabEof(sqlite3_vtab_cursor* pVtabCursor) {
	EsentVtabCursor* pCur = (EsentVtabCursor*)pVtabCursor;
	EsentVtab* pTab = (EsentVtab*)pVtabCursor->pVtab;
	return pCur->moveErr;
}

static int esentVtabColumn(
	sqlite3_vtab_cursor* pVtabCursor,
	sqlite3_context* ctx,
	int i
) {
	EsentVtabCursor* pCur = (EsentVtabCursor*)pVtabCursor;
	EsentVtab* pTab = (EsentVtab*)pVtabCursor->pVtab;
	int rc = SQLITE_OK;
	JET_ERR jrc = JET_errSuccess;

	char buf[4096] = { 0 }; // TODO resize if needed
	unsigned long cbActual = 0;

	jrc = JetRetrieveColumn(pTab->jses, pCur->jtab, pTab->colIds[i], buf, sizeof(buf), &cbActual, 0, NULL);
	if (jrc != JET_errSuccess)
		return SQLITE_ERROR;

	int colTyp = pTab->colTyps[i];

	switch (colTyp) {
	case JET_coltypNil:
		sqlite3_result_null(ctx);
		break;
	case JET_coltypBit:
		sqlite3_result_int(ctx, buf[0]); // FIXME header does not specify stoarge of bit
		break;
	case JET_coltypUnsignedByte:
		sqlite3_result_int(ctx, buf[0]);
		break;
	case JET_coltypShort:
	case JET_coltypUnsignedShort:
		sqlite3_result_int(ctx, ((short*)buf)[0]);
		break;
	case JET_coltypLong:
	case JET_coltypUnsignedLong:
		sqlite3_result_int(ctx, ((int*)buf)[0]);
		break;
	case JET_coltypCurrency:
	case JET_coltypLongLong:
	case JET_coltypUnsignedLongLong:
		sqlite3_result_int64(ctx, ((sqlite3_int64*)buf)[0]);
		break;
	case JET_coltypIEEESingle:
		sqlite3_result_double(ctx, ((float*)buf)[0]);
		break;
	case JET_coltypIEEEDouble:
		sqlite3_result_double(ctx, ((double*)buf)[0]);
		break;
	case JET_coltypDateTime:
	case JET_coltypBinary:
	case JET_coltypLongBinary:
	case JET_coltypGUID:
		sqlite3_result_blob(ctx, buf, cbActual, SQLITE_TRANSIENT);
		break;
	case JET_coltypText:
	case JET_coltypLongText:
		sqlite3_result_text16(ctx, buf, cbActual, SQLITE_TRANSIENT);
		break;
	case JET_coltypSLV:
	default:
		rc = SQLITE_ERROR;
		pTab->base.zErrMsg = sqlite3_mprintf("Invalid JET type: %d", colTyp);
		break;
	}

	return rc;
}

static int esentVtabNext(sqlite3_vtab_cursor* pVtabCursor) {
	EsentVtabCursor* pCur = (EsentVtabCursor*)pVtabCursor;
	EsentVtab* pTab = (EsentVtab*)pVtabCursor->pVtab;
	int rc = SQLITE_OK;
	JET_ERR jrc = JET_errSuccess;

	jrc = JetMove(pTab->jses, pCur->jtab, JET_MoveNext, 0);
	pCur->moveErr = jrc;
	if (jrc != JET_errSuccess && jrc != JET_errNoCurrentRecord) {
		return SQLITE_ERROR;
	}

	return rc;
}

static sqlite3_module esentModule = {
	.iVersion = 0,
	.xCreate = esentVtabCreate,
	.xConnect = esentVtabConnect,
	.xDisconnect = esentVtabDisconnect,
	.xDestroy = esentVtabDestroy,
	.xBestIndex = esentVtabBestIndex,
	.xOpen = esentVtabOpen,
	.xFilter = esentVtabFilter,
	.xEof = esentVtabEof,
	.xClose = esentVtabClose,
	.xColumn = esentVtabColumn,
	.xNext = esentVtabNext,
};

static void wintimeFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
	sqlite_int64 v = sqlite3_value_int64(argv[0]);
	sqlite_int64 ts = (v - 116444736000000000L) / 10000000L;
	sqlite3_result_int64(ctx, ts);
}

static int createEsentModule(sqlite3* db) {
	int rc;
	rc = sqlite3_create_module_v2(db, "esentvtab", &esentModule, NULL, NULL);
	sqlite3_create_function_v2(db, "wintime", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, wintimeFunc, NULL, NULL, NULL);
	return rc;
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_extension_init(
	sqlite3* db,
	char** pzErrMsg,
	const sqlite3_api_routines* pApi
) {
	int rc = SQLITE_OK;
	SQLITE_EXTENSION_INIT2(pApi);
	(void)pzErrMsg;  /* Suppress harmless warning */
	fprintf(stderr, "Loaded esent\n");
	rc = createEsentModule(db);

	return rc;
}
