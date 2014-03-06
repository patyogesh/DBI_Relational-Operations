#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"
#include "GenDBFile.h"
#include "Defs.h"
#include "BigQ.h"

using namespace std;
#include <fstream>
#include <iostream>
#include <string.h>

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
  char path[100];

  sprintf(path, "%s.metadata", f_path);
  FILE *fptr = fopen(path, "wr");

  switch(f_type) {
    case heap:
      {
        gen_db_file_ptr = new HeapFile();
        fwrite((int *)&f_type, sizeof(f_type), 1, fptr);
      }
      break;
    case sorted:
      {
        gen_db_file_ptr = new SortedFile();

        fwrite((int *)&f_type, sizeof(f_type), 1, fptr);

        SortInfo *si = (SortInfo *)startup;
        fwrite((SortInfo *)si, sizeof(SortInfo), 1, fptr);
      }
      break;
    case tree:
      break;
    default:
      cerr<<"\n Unknown input file type";
      exit(1);
  }
  
  fclose(fptr);

  return gen_db_file_ptr->Create(f_path, f_type, startup);
}

void DBFile::Load (Schema &f_schema, char *loadpath) {

  gen_db_file_ptr->Load(f_schema, loadpath);
}

int DBFile::Open (char *f_path) {
  char path[100];
  fType f_type;
  
  sprintf(path, "%s.metadata", f_path);
  
  FILE *fptr = fopen(path, "r");
  if(!fread(&f_type, sizeof(f_type), 1, fptr)) {
    cerr<<"\n Read Error";
    exit(1);
  }

  switch(f_type) {
    case heap:
      gen_db_file_ptr = new HeapFile();
      break;
    case sorted:
      gen_db_file_ptr = new SortedFile();
      break;
    case tree:
      break;
    default:
      cerr<<"\n Unknown input file type";
      exit(1);
  }
  
  fclose(fptr);

  return gen_db_file_ptr->Open(f_path);
}

int DBFile::Close () {
  /*
   * Close .bin file
   */
  gen_db_file_ptr->Close();
}

void DBFile::MoveFirst () {
  gen_db_file_ptr->MoveFirst();
}

void DBFile::Add (Record &rec) {
  gen_db_file_ptr->Add(rec);
}

int DBFile::GetNext (Record &fetchme)
{
  return gen_db_file_ptr->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &myComparison, Record &literal) {
  return gen_db_file_ptr->GetNext(fetchme, myComparison, literal);
}