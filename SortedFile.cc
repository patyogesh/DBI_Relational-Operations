#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "GenDBFile.h"
#include "SortedFile.h"
#include "Defs.h"
#include "BigQ.h"

using namespace std;
#include <fstream>
#include <iostream>
#include <string.h>

SortedFile::SortedFile () {
  pageReadInProg = 0;
  currPageIndex = 0;
}

int SortedFile::Create (char *f_path, fType f_type, void *startup) {
  /*
   * Create .bin file if doesn't exist
   * Open .bin file
   */
  checkIsFileOpen.open(f_path,ios_base::out | ios_base::in);

  if(checkIsFileOpen.is_open()) {
    currFile.Open(1, f_path);
  }
  else {
    currFile.Open(0, f_path);
  }
  return 1;
}

void SortedFile::Load (Schema &f_schema, char *loadpath) {

  /*
   * Open .tbl file
   */
  tblFile = fopen(loadpath, "rb");

  if(!tblFile) {
    cout << "\nFailed to Open the file: %s" << loadpath;
    return;
  }

  currRecord = new (std::nothrow) Record;

  int appendStatus = 1;

  /*
   * Read record(s) from .tbl file One at a time
   * till EOF is reached
   */
  while(currRecord->SuckNextRecord(&f_schema, tblFile)) {

      /*
       * Append the sucked record to page
       */
      appendStatus = currPage.Append(currRecord);

      /*
       * If page is full, write the page to file
       */
      if(0 == appendStatus) {


        currFile.AddPage(&currPage, currFile.GetLength());

        appendStatus = 1;

        /*
         * Flush the page to re-use it to store further records
         */
        currPage.EmptyItOut();
        currPage.Append(currRecord);
      }
  }

  /*
   * Wri te this page to file although not full because,
   * we have sucked all records from file
   */
  currFile.AddPage(&currPage, currFile.GetLength());

  /*
   * Free temporary buffer
   */
  delete currRecord;
}

int SortedFile::Open (char *f_path) {
  /*
   * read sortOrder and runLen from .metadata file
   */
  char path[100];
  fType f_type;
  sprintf(path, "%s.metadata", f_path);

  FILE *fptr = fopen(path, "r");
  SortInfo si;                                                                                                                                                    

  if(!fread(&f_type,sizeof(fType), 1, fptr)) {
    cerr<<"\n f_type Read Error";
    exit(1);
  }
  if(!fread(&si,sizeof(SortInfo), 1, fptr)) {
    cerr<<"\n SortInfo Read Error";
    exit(1);
  }
  /*
   * Read from file and store the sortorder info and
   * runLen in private members of SortedFile
   */
  sortOrder = new OrderMaker;
  sortOrder = si.myOrder;
  runLen    = si.runLength;
  fclose(fptr);
  /*
   * Create .bin file if doesn't exist
   * Open .bin file
   */
  checkIsFileOpen.open(f_path,ios_base::out | ios_base::in);

  if(checkIsFileOpen.is_open()) {
    currFile.Open(1, f_path);
  }
  else {
    currFile.Open(0, f_path);
  }
  
  return 1;
}

int SortedFile::Close () {
  /*
   * Close .bin file
   */
  currFile.Close();
}

void SortedFile::MoveFirst () {

  /*
   * Check if file really contain any records
   */

//	cout << " Move First";
  if(currFile.GetLength()==0){
    cout << "Bad operation , File Empty" ;
  }
  else{
//	  cout << " Inside DB FIle Move First currPageIndex : "<<currPageIndex<<endl;
    currPageIndex = 0;
    currFile.MoveFirst();
    currFile.GetPage(&currPage, currPageIndex++);
    pageReadInProg = 1;
  }
}

void SortedFile::Add (Record &rec) {

  inPipe = new Pipe(IN_OU_PIPE_BUFF_SIZE);
  outPipe = new Pipe(IN_OU_PIPE_BUFF_SIZE);

  inPipe->Insert(&rec);
#if 0
  if(pageReadInProg==0) {
    currFile.AddPage(&currPage, currFile.GetLength());
    pageReadInProg = 1;
  }


  if(currFile.GetLength()>0) //existing page
  {
    currFile.GetPage(&currPage,currFile.GetLength()-2);
    currPageIndex = currFile.GetLength()-2;
  }
  if(!currPage.Append(&rec)) //full page
  {
    currPage.EmptyItOut();
    currPage.Append(&rec);
    currPageIndex++;
  }

  currFile.AddPage(&currPage,currPageIndex);
#endif
}

int SortedFile::GetNext (Record &fetchme)
{
#if DEBUG
  cout<< " current page index :" << currPageIndex << endl;
  cout<< " current page length :" << currFile.GetLength() << endl;
#endif
  if(pageReadInProg==0) {
    // currPageIndex = 460;
//	  cout << "GetPage 1"<< currPageIndex << endl;
    currFile.GetPage(&currPage, currPageIndex);
    currPageIndex= currPageIndex +1;
    pageReadInProg = 1;
  }


  //fetch page

  if(currPage.GetFirst(&fetchme) ) {
    return 1;
  }
  else{

    if(!(currPageIndex > currFile.GetLength()-2))
    {//cout << "GetPage 2  page index :"<< currPageIndex << endl;
      currFile.GetPage(&currPage, currPageIndex++);
      pageReadInProg++;
      currPage.GetFirst(&fetchme);
      return 1;
    }
    else{
      return 0;
    }
  }
}

int SortedFile::GetNext (Record &fetchme, CNF &myComparison, Record &literal) {

	/*
   * now open up the text file and start procesing it
   * read in all of the records from the text file and see if they match
	 * the CNF expression that was typed in
   */
	 ComparisonEngine comp;

	 while(GetNext(fetchme)){
		if(comp.Compare (&fetchme, &literal, &myComparison)==1)
			return 1;
	}

  return 0;

}
