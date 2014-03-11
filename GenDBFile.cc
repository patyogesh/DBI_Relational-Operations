#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenDBFile.h"
#include "DBFile.h"
#include "Defs.h"

using namespace std;
#include <fstream>
#include <iostream>
#include <string.h>

GenDBFile::GenDBFile () {
#if 0
  pageReadInProg = 0;
  currPageIndex = 0;
#endif
}
GenDBFile::~GenDBFile () {
}

int GenDBFile::Create (char *f_path, fType f_type, void *startup) {
}

void GenDBFile::Load (Schema &f_schema, char *loadpath) {
}

int GenDBFile::Open (char *f_path) {
}

int GenDBFile::Close () {
}

void GenDBFile::MoveFirst () {
}

void GenDBFile::Add (Record &rec) {
  cout<<"\n ===  GenDBFile::Add currMode: ===";
}

int GenDBFile::GetNext (Record &fetchme)
{
}

int GenDBFile::GetNext (Record &fetchme, CNF &myComparison, Record &literal) {
}

void GenDBAppendSequential(Record &appendme){

}
