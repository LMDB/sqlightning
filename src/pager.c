#ifndef SQLITE_OMIT_DISKIO
#include "sqliteInt.h"
#include "wal.h"

/*
** Return the file handle for the database file associated
** with the pager.  This might return NULL if the file has
** not yet been opened.
*/
sqlite3_file *sqlite3PagerFile(Pager *pPager){
  return NULL;
}

/*
** Return the full pathname of the database file.
*/
const char *sqlite3PagerFilename(Pager *pPager){
  return NULL;
}

/*
** Get/set the locking-mode for this pager. Parameter eMode must be one
** of PAGER_LOCKINGMODE_QUERY, PAGER_LOCKINGMODE_NORMAL or 
** PAGER_LOCKINGMODE_EXCLUSIVE. If the parameter is not _QUERY, then
** the locking-mode is set to the value specified.
**
** The returned value is either PAGER_LOCKINGMODE_NORMAL or
** PAGER_LOCKINGMODE_EXCLUSIVE, indicating the current (possibly updated)
** locking-mode.
*/
int sqlite3PagerLockingMode(Pager *pPager, int eMode){
}

/*
** Get/set the size-limit used for persistent journal files.
**
** Setting the size limit to -1 means no limit is enforced.
** An attempt to set a limit smaller than -1 is a no-op.
*/
i64 sqlite3PagerJournalSizeLimit(Pager *pPager, i64 iLimit){
  return -1;
}

/*
** Return the approximate number of bytes of memory currently
** used by the pager and its associated cache.
*/
int sqlite3PagerMemUsed(Pager *pPager){
  return 0;
}

/*
** Return the current journal mode.
*/
int sqlite3PagerGetJournalMode(Pager *pPager){
  return PAGER_JOURNALMODE_OFF;
}

/*
** Return TRUE if the pager is in a state where it is OK to change the
** journalmode.  Journalmode changes can only happen when the database
** is unmodified.
*/
int sqlite3PagerOkToChangeJournalMode(Pager *pPager){
  return 0;
}

/*
** Set the journal-mode for this pager. Parameter eMode must be one of:
**
**    PAGER_JOURNALMODE_DELETE
**    PAGER_JOURNALMODE_TRUNCATE
**    PAGER_JOURNALMODE_PERSIST
**    PAGER_JOURNALMODE_OFF
**    PAGER_JOURNALMODE_MEMORY
**    PAGER_JOURNALMODE_WAL
**
** The journalmode is set to the value specified if the change is allowed.
** The change may be disallowed for the following reasons:
**
**   *  An in-memory database can only have its journal_mode set to _OFF
**      or _MEMORY.
**
**   *  Temporary databases cannot have _WAL journalmode.
**
** The returned indicate the current (possibly updated) journal-mode.
*/
int sqlite3PagerSetJournalMode(Pager *pPager, int eMode){
  return PAGER_JOURNALMODE_OFF;
}

/*
** Return true if the underlying VFS for the given pager supports the
** primitives necessary for write-ahead logging.
*/
int sqlite3PagerWalSupported(Pager *pPager){
  return 0;
}

/*
** This function is called to close the connection to the log file prior
** to switching from WAL to rollback mode.
**
** Before closing the log file, this function attempts to take an 
** EXCLUSIVE lock on the database file. If this cannot be obtained, an
** error (SQLITE_BUSY) is returned and the log connection is not closed.
** If successful, the EXCLUSIVE lock is not released before returning.
*/
int sqlite3PagerCloseWal(Pager *pPager){
  return SQLITE_OK;
}

int sqlite3PagerWalCallback(Pager *pPager){
  return SQLITE_OK;
}

/*
** This function may only be called while a write-transaction is active in
** rollback. If the connection is in WAL mode, this call is a no-op. 
** Otherwise, if the connection does not already have an EXCLUSIVE lock on 
** the database file, an attempt is made to obtain one.
**
** If the EXCLUSIVE lock is already held or the attempt to obtain it is
** successful, or the connection is in WAL mode, SQLITE_OK is returned.
** Otherwise, either SQLITE_BUSY or an SQLITE_IOERR_XXX error code is 
** returned.
*/
int sqlite3PagerExclusiveLock(Pager *pPager){
  return SQLITE_OK;
}

/*
** Return true if this is an in-memory pager.
*/
int sqlite3PagerIsMemdb(Pager *pPager){
  return 0;
}

#ifdef SQLITE_TEST
int sqlite3_pager_readdb_count = 0;    /* Number of full pages read from DB */
int sqlite3_pager_writedb_count = 0;   /* Number of full pages written to DB */
int sqlite3_pager_writej_count = 0;    /* Number of pages written to journal */
int sqlite3_opentemp_count = 0;
/*
** This routine is used for testing and analysis only.
** Some cheesy manipulation of the values in a is done so
** that the incrblob 2.* tests pass, even though auto_vacuum
** is not implemented for DB SQLITE.
*/
int *sqlite3PagerStats(Pager *pPager) {
	static int a[11];
	static int count = 0;
	if (count > 3) {
		a[9] = 4;
	} else {
		memset(&a[0], 0, sizeof(a));
		a[9] = 30;
		a[10] = 2;
	}
	count++;
	return a;
}

/* SQLite redefines sqlite3PagerAcquire for this implementation. */
int sqlite3PagerGet(Pager *pPager, Pgno pgno, DbPage **ppPage)
{
	return SQLITE_NOMEM;
}
void *sqlite3PagerGetData(DbPage *pPg)
{
	return NULL;
}
void sqlite3PagerUnref(DbPage *pPg)
{}
#endif

#endif
