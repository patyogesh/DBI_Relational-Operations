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
  file_path = f_path;
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
  file_path = f_path;
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
   * Close .bin file in case when we are just scanning
   * through file OR
   * close after successful creation of heap file
   */
  if(!outPipe) {
    return currFile.Close();
  }

  /*
   * If control falls through here,
   * we are dealing with sorted file
   */

	if(inPipe)
	  inPipe->ShutDown ();

	ComparisonEngine ceng;

	DBFile dbfile;
	char outfile[100];

	sprintf (outfile, "%s.bigq", file_path);
	dbfile.Create (outfile, heap, NULL);

	int err = 0;
	int i = 0;

	Record rec;
	Record *last = NULL, *prev = NULL;

	while (outPipe->Remove (&rec)) {
		dbfile.Add (*&rec);
		i++;
	}

	cout << "\n consumer: removed " << i << " recs from the pipe\n";

	cerr << "\n consumer: recs removed written out as heap DBFile at " << outfile << endl;
	dbfile.Close ();

  delete inPipe;
  delete outPipe;
  inPipe = NULL;
  outPipe = NULL;

  currFile.Close();
  /*
   * Remove XX.bin file and
   * Rename XX.bin.bigq to XX.bin
   */
  remove(file_path);
  rename(outfile, file_path);

  /*
   * Need to be handled more gracefully
   * the unwanted XX.bigq.metadata being created.
   * One way is to call Append and AddPage APIs instead of
   * dbfile.Add()
   */
	sprintf (outfile, "%s.metadata", outfile);
  remove(outfile);
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
      	//cout << "Create queue ";
      flag=1;
    }
  }
  else if(WRITING == currMode) {
    inPipe->Insert(&rec);
  }
  else {
  }
}

void SortedFile::mergeInflghtRecs(){

//	int pipeover=0, fileover = 0;

	// shutdown input pipe
	inPipe->ShutDown();
//	file.Close();

	//empty the ouput pipe and do a two way merge

	// remove records from BigQ output pipe one at a time
	// at the same time u are removing records from the internal BigQ you scan the sorted files data in sorted order
	// use a standard two way merge algorithm u merge the two sets of data and write the result out to a new instance of the file Class

	Record *pipeRec;
	Record *fileRec;



	ComparisonEngine comp;
	DBFile tmp;

	time_t seconds;

	seconds = time (NULL);

	cout << "time in sseconds :"<< seconds ;
	char* filename = "mergeFile"+seconds;

	tmp.Open(filename);
	//tmp.Open(0,filename);

	Page tmpPage = new Page();

	int fromPipe = 0, fromFile =0;
	//currFile.MoveFirst();

	DBFile dbfile;
	dbfile.Open (file_path);
	dbfile.MoveFirst ();

	while(1){

		pipeRec = new Record;
		fileRec = new Record;

		fromPipe = outPipe->Remove(pipeRec);
		fromFile = dbfile.GetNext(*fileRec);

		if(fromPipe && fromFile){
			if(comp.Compare(pipeRec,fileRec,sortOrder) > 0){
				tmp.Add(*fileRec);
			}
			else{
				tmp.Add(*pipeRec);
			}

			continue;

		}/*
		else{
			break;
		}
		*/
		if(fromPipe && !fromFile){
			tmp.Add(*pipeRec);
		}
		else{
			tmp.Add(*fileRec);
		}

		if(!fromPipe && !fromFile){
			break;
		}




	}






	}

void SortedFile::toggleCurrMode(){

	if(READING == currMode) {
		   currMode = WRITING;
	}
	else{
		   currMode = READING;
	}

}

void SortedFile::createMetaFile()
{
	char path[100];
	  fType f_type;
	  sprintf(path, "%s.metadata", file_path);


	ofstream metaFile;
		metaFile.open(path);
		metaFile << "Sorted"<< endl;
		metaFile << (sortInfo)->runLength<< endl;

		metaFile << ((sortInfo)->myOrder->numAtts)<<endl;

}

int SortedFile::GetNext (Record &fetchme)
{
#if DEBUG
  cout<< " current page index :" << currPageIndex << endl;
  cout<< " current page length :" << currFile.GetLength() << endl;
#endif

  if(WRITING == currMode) {
	  // change mode and merge the inflight
	  toggleCurrMode();
	  mergeInflghtRecs();
  }
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

	// variables for Binary Search
	int start=0 , end=currFile.GetLength() - 2 ,mid=0;

	if(WRITING == currMode) {
		  // change mode and merge the inflight
		  toggleCurrMode();
		  mergeInflghtRecs();
	  }

	if(pageReadInProg!=0) {
		currFile.MoveFirst();
	}

	OrderMaker *query = GetMatchingOrder(myComparison,*(sortInfo->myOrder));

	query->Print();

		if (!query)
		{
		    ComparisonEngine compEngine;
			while (GetNext(fetchme))
		    {
	    	    if (compEngine.Compare(&fetchme, &literal, &myComparison))
	        	    return 1;
			}
			//if control is here then no matching record was found
		    return 0;
		}
		else
		{
	        ComparisonEngine compEngine;

			// Binary search must be done once per query-order-maker
			if (bSearchFlg == 0)
			{
				bSearchFlg = 1;
				LoadMatchingPage(literal);
				// find the page where query-order-maker first matches (using binary search)
	            bool foundMatchingRec = false;
	            while(GetNext(fetchme))
	            {
	                // match with queryOM, until we find a matching record
	                if (compEngine.Compare(&literal, query, &fetchme, sortInfo->myOrder) == 0)
	                {
	                    if (compEngine.Compare(&fetchme, &literal, &myComparison))
	                        return 1;
	                    foundMatchingRec = true;
	                    break;
	                }
	            }
	            if(!foundMatchingRec)
	                return 0;
			}

			while(true)
	        {
	        	if(GetNext(fetchme))
	            {
	            	// match with queryOM, if matches, compare with CNF
	                if (compEngine.Compare(&literal, query, &fetchme, sortInfo->myOrder) == 0)
	                {
	                	if (compEngine.Compare(&fetchme, &literal, &myComparison))
	                    	return 1;
						// otherwise CNF didn't match, try next record
	                }
	                else
	                {
	                	//if queryOM doesn't match, stop searching, return false
	                    return 0;
	                }
				}
	            else
	            	return 0;
			}
		}

	    //if control is here then no matching record was found
	    return 0;

}

OrderMaker* SortedFile:: GetMatchingOrder(CNF &myComparison,OrderMaker& file_order)
{
	/*
	 * xamine the CNF that you are passed

to try to build up an instance of the OrderMaker class that you can use to run a binary search on the sorted file.


   OrderMakers {
     int numAtts;
     int whichAtts[MAX_ANDS];
     Type whichTypes[MAX_ANDS];
  }
   CNFs{
     Comparison orList[MAX_ANDS][MAX_ORS];
     int orLens[MAX_ANDS];
     int numAnds;
}
   check if cached order maker is usable
      create new ordermaker if not
	 *
	 *
	 *
	 */
    OrderMaker cnf_order;
    OrderMaker fileOrderCopy = file_order;
    GetSortOrderFromCNF(myComparison,cnf_order, fileOrderCopy);

    OrderMaker *query = new OrderMaker();

    for (int i = 0; i < file_order.numAtts; i++)
    {
        bool matched = false;
        for(int j = 0; j < cnf_order.numAtts; j++)
        {
            if((file_order.whichAtts[i] == cnf_order.whichAtts[j]) && (file_order.whichTypes[i] == cnf_order.whichTypes[j]))
            {
                matched = true;
                query->whichAtts[query->numAtts] = fileOrderCopy.whichAtts[j];
                query->whichTypes[query->numAtts] = fileOrderCopy.whichTypes[j];
                query->numAtts++;
                break;
            }
        }
        if(!matched)
            break;
    }
    if(query->numAtts > 0)
        return query;
    else
    {
        delete query;
        return NULL;
    }

}

int SortedFile:: GetSortOrderFromCNF (CNF &myComparison,OrderMaker &cnf_order, OrderMaker &file_order) {

    cnf_order.numAtts = 0;
    file_order.numAtts = 0;

    for (int i = 0; i < myComparison.numAnds; i++)
	{
        if (myComparison.orLens[i] != 1) {
            continue;
        }

        if (myComparison.orList[i][0].op != Equals) {
            continue;
        }

        if (myComparison.orList[i][0].operand1 == Left && myComparison.orList[i][0].operand2 == Literal)
        {
            cnf_order.whichAtts[cnf_order.numAtts] = myComparison.orList[i][0].whichAtt1;
            cnf_order.whichTypes[cnf_order.numAtts] = myComparison.orList[i][0].attType;
            file_order.whichAtts[file_order.numAtts] = myComparison.orList[i][0].whichAtt2;
            file_order.whichTypes[file_order.numAtts] = myComparison.orList[i][0].attType;
        }

        else if (myComparison.orList[i][0].operand1 == Literal && myComparison.orList[i][0].operand2 == Right)
        {
            cnf_order.whichAtts[cnf_order.numAtts] = myComparison.orList[i][0].whichAtt2;
            cnf_order.whichTypes[cnf_order.numAtts] = myComparison.orList[i][0].attType;
            file_order.whichAtts[file_order.numAtts] = myComparison.orList[i][0].whichAtt1;
            file_order.whichTypes[file_order.numAtts] = myComparison.orList[i][0].attType;
        }
        else
            continue;

        cnf_order.numAtts++;
        file_order.numAtts++;
    }

    return cnf_order.numAtts;

}

