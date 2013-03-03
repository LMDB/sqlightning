#include "sqliteInt.h"
#include "lmdb.h"

/* A Btree handle
**
** A database connection contains a pointer to an instance of
** this object for every database file that it has open.  This structure
** is opaque to the database connection.  The database connection cannot
** see the internals of this structure and only deals with pointers to
** this structure.
**
** For some database files, the same underlying database cache might be 
** shared between multiple connections.  In that case, each connection
** has it own instance of this object.  But each instance of this object
** points to the same BtShared object.  The database cache and the
** schema associated with the database file are all contained within
** the BtShared object.
**
** All fields in this structure are accessed under sqlite3.mutex.
** The pBt pointer itself may not be changed while there exists cursors 
** in the referenced BtShared that point back to this Btree since those
** cursors have to go through this Btree to find their BtShared and
** they often do so without holding sqlite3.mutex.
*/
struct Btree {
  sqlite3 *db;       /* The database connection holding this btree */
  BtShared *pBt;     /* Sharable content of this btree */
  BtCursor *pCursor;
  MDB_txn *main_txn;
  MDB_txn *curr_txn;
  u8 inTrans;        /* TRANS_NONE, TRANS_READ or TRANS_WRITE */
  u8 locked;         /* True if db currently has pBt locked */
  u8 isTemp;		 /* a temporary DB */
  int wantToLock;    /* Number of nested calls to sqlite3BtreeEnter() */
  Btree *pNext;      /* List of other sharable Btrees from the same db */
  Btree *pPrev;      /* Back pointer of the same list */
};

/*
** Btree.inTrans may take one of the following values.
**
** If the shared-data extension is enabled, there may be multiple users
** of the Btree structure. At most one of these may open a write transaction,
** but any number may have active read transactions.
*/
#define TRANS_NONE  0
#define TRANS_READ  1
#define TRANS_WRITE 2

/*
** An instance of this object represents a single database file.
** 
** A single database file can be in use at the same time by two
** or more database connections.  When two or more connections are
** sharing the same database file, each connection has it own
** private Btree object for the file and each of those Btrees points
** to this one BtShared object.  BtShared.nRef is the number of
** connections currently sharing this database file.
**
** Fields in this structure are accessed under the BtShared.mutex
** mutex, except for nRef and pNext which are accessed under the
** global SQLITE_MUTEX_STATIC_MASTER mutex.  The pPager field
** may not be modified once it is initially set as long as nRef>0.
** The pSchema field may be set once under BtShared.mutex and
** thereafter is unchanged as long as nRef>0.
**
** isPending:
**
**   If a BtShared client fails to obtain a write-lock on a database
**   table (because there exists one or more read-locks on the table),
**   the shared-cache enters 'pending-lock' state and isPending is
**   set to true.
**
**   The shared-cache leaves the 'pending lock' state when either of
**   the following occur:
**
**     1) The current writer (BtShared.pWriter) concludes its transaction, OR
**     2) The number of locks held by other connections drops to zero.
**
**   while in the 'pending-lock' state, no connection may start a new
**   transaction.
**
**   This feature is included to help prevent writer-starvation.
*/
struct BtShared {
  sqlite3 *db;          /* Database connection currently using this Btree */
  Btree *trees;			/* All Btree handles using this struct */
  MDB_env *env;
  char *lockname;		/* name of MDB lockfile */
  u8 openFlags;         /* Flags to sqlite3BtreeOpen() */
  u8 inTransaction;     /* Transaction state */
  int nTransaction;     /* Number of open transactions (read + write) */
  void *pSchema;        /* Pointer to space allocated by sqlite3BtreeSchema() */
  void (*xFreeSchema)(void*);  /* Destructor for BtShared.pSchema */
  sqlite3_mutex *mutex; /* Non-recursive mutex required to access this object */
#ifndef SQLITE_OMIT_SHARED_CACHE
  int nRef;             /* Number of references to this structure */
  BtShared *pNext;      /* Next on a list of sharable BtShared structs */
  Btree *pWriter;       /* Btree with currently open write transaction */
#ifdef SQLITE_TEST
  void *pPager;			/* dummy */
#endif
#endif
};

/*
** A cursor is a pointer to a particular entry within a particular
** b-tree within a database file.
**
** The entry is identified by its MemPage and the index in
** MemPage.aCell[] of the entry.
**
** A single database file can be shared by two more database connections,
** but cursors cannot be shared.  Each cursor is associated with a
** particular database connection identified BtCursor.pBtree.db.
**
** Fields in this structure are accessed under the BtShared.mutex
** found at self->pBt->mutex. 
*/
struct BtCursor {
  BtCursor *pNext;
  Btree *pBtree;
  struct KeyInfo *pKeyInfo; /* Argument passed to comparison function */
  sqlite3_int64 cachedRowid; /* Next rowid cache.  0 means not valid */
  MDB_val index;		/* reconstructed Index key */
};
