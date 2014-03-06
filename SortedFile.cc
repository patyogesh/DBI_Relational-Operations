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
  counter=0;
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

  currMode = WRITING;

  if(!bigQ) {
    inPipe = new Pipe(IN_OUT_PIPE_BUFF_SIZE);
    outPipe = new Pipe(IN_OUT_PIPE_BUFF_SIZE);
    bigQ = new BigQ(*inPipe, *outPipe, *sortOrder, runLen);
  }
    
  /*
   * Open .tbl file
   */
  tblFile = fopen(loadpath, "rb");

  if(!tblFile) {
    cerr << "\nFailed to Open the file: %s" << loadpath;
    return;
  }

  currRecord = new (std::nothrow) Record;

  /*
   * Read record(s) from .tbl file One at a time
   * till EOF is reached
   */
  while(currRecord->SuckNextRecord(&f_schema, tblFile)) {
    inPipe->Insert(currRecord);
  }
  delete currRecord;

}

int SortedFile::Open (char *f_path) {
#ifdef DEBUG
  cout<<"\n ===  SortedFile::Open currMode: " << currMode <<"===";
#endif
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
  
  currMode = READING;
#ifdef DEBUG
  cout<<"\n === Open currMode: READING ===";
#endif
  MoveFirst();
  return 1;
}

int SortedFile::Close () {
  /*
   * Close .bin file
   */
	if(inPipe)
	inPipe->ShutDown ();
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



void*
bigQueue1(void *vptr) {
	cout << "Creating Thread ";
	threadParams_t *inParams = (threadParams_t *) vptr;
	BigQ bq(*inParams->inPipe, *inParams->outPipe, *inParams->sortOrder, inParams->runLen);
}
void SortedFile::Add (Record &rec) {

  counter++;
 // cout<<"\n#### Counter:" << counter << &rec <<"####";
  if(READING == currMode) {
    currMode = WRITING;

    if(flag==0) {
      inPipe = new Pipe(IN_OUT_PIPE_BUFF_SIZE);
      outPipe = new Pipe(IN_OUT_PIPE_BUFF_SIZE);
      inPipe->Insert(&rec);
      pthread_t thread1;

  	threadParams_t *tp = new (std::nothrow) threadParams_t;

  	/*
  	 * use a container to pass arguments to worker thread
  	 */
  	tp->inPipe = inPipe;
  	tp->outPipe = outPipe;
  	tp->sortOrder = sortOrder;
  	tp->runLen = runLen;

  	pthread_create(&thread1, NULL, bigQueue1, (void *) tp);
      	cout << "Create queue ";
      //	flag=1;



    }

  }
  else if(WRITING == currMode) {
	  cout << " in else";
    inPipe->Insert(&rec);

  }
  else {
  }
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
