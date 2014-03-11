#ifndef GEN_DB_FILE_H
#define GEN_DB_FILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <fstream>

class GenDBFile {
    int       pageReadInProg; /* flag to indicate if page is read from file */
    int       currPageIndex;  /* Index of page currently being read */
    FILE      *dbFile;        /* Pointer to DB file */
    FILE      *tblFile;       /* Pointer to TBL file */
    int       numRecordsRead; /* Number of records read from Page */
    int       numPagesRead;   /* Number of pages read from file */
    Record    *currRecord;    /* Pointer to current record being read/written */
    Page      currPage;       /* Pointer to current page being read/written */
    File      currFile;       /* Pointer to current file being read/written */
    fstream   checkIsFileOpen;/* flag to check if file already open */

public:
    GenDBFile ();
    virtual ~GenDBFile() =0;

    virtual int Create (char *fpath, fType file_type, void *startup)=0;
    virtual int Open (char *fpath)=0;
    virtual int Close ()=0;

    virtual void Load (Schema &myschema, char *loadpath)=0;

    virtual void MoveFirst ()=0;
    virtual void Add (Record &addme)=0;
    virtual int GetNext (Record &fetchme)=0;
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal)=0;
    virtual void AppendSequential(Record &appendme)=0;

};
#endif
