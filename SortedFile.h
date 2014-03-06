#ifndef SORTED_FILE_H
#define SORTED_FILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenDBFile.h"
#include "Defs.h"
#include "Pipe.h"
#include "BigQ.h"
#include <fstream>

#define IN_OUT_PIPE_BUFF_SIZE 100

enum SortedFileMode
{
  READING,
  WRITING
};

class SortedFile:public GenDBFile {
    int       counter;
    int flag =0;
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

    BigQ      *bigQ;
    Pipe      *inPipe;
    Pipe      *outPipe;
    OrderMaker *sortOrder;
    int       runLen;
    SortedFileMode  currMode;

public:
    SortedFile ();

    int Create (char *fpath, fType file_type, void *startup);
    int Open (char *fpath);
    int Close ();

    void Load (Schema &myschema, char *loadpath);

    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    void *setupBq(void *ptr);
    void start();
};
#endif
