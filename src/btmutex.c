#include "btreeInt.h"
#ifndef SQLITE_OMIT_SHARED_CACHE
/*
 * MDB uses MVCC - disable the SQLite table level locking.
 */
void sqlite3BtreeEnter(Btree *p) { }

void sqlite3BtreeLeave(Btree *p) { }

void sqlite3BtreeEnterCursor(BtCursor *pCur) { }

void sqlite3BtreeLeaveCursor(BtCursor *pCur) { }

void sqlite3BtreeEnterAll(sqlite3 *db) { }

void sqlite3BtreeLeaveAll(sqlite3 *db) { }

int sqlite3BtreeHoldsMutex(Btree *db)
{
	return 1;
}

struct BtreeMutexArray;

typedef struct BtreeMutexArray BtreeMutexArray;
void sqlite3BtreeMutexArrayEnter(BtreeMutexArray *pArray) { }

void sqlite3BtreeMutexArrayLeave(BtreeMutexArray *pArray) { }

/*
** Return true if a particular Btree requires a lock.  Return FALSE if
** no lock is ever required since it is not sharable.
*/
int sqlite3BtreeSharable(Btree *p){
  return 0;
}

int sqlite3BtreeHoldsAllMutexes(sqlite3 *db)
{
	return 1;
}

void sqlite3BtreeMutexArrayInsert(BtreeMutexArray *pArray, Btree *pBtree) { }
#endif /* ifndef SQLITE_OMIT_SHARED_CACHE */
