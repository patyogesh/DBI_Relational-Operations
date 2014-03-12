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
#include <cstdio>
#include <sys/time.h>
#include <sstream>

using namespace std;
#include <fstream>
#include <iostream>
#include <string.h>

SortedFile::SortedFile() {
	pageReadInProg = 0;
	currPageIndex = 0;
	counter = 0;
  hasSortOrder = 1;
  flag = 0;
  found = 0;
}

int SortedFile::Create(char *f_path, fType f_type, void *startup) {
	/*
	 * Create .bin file if doesn't exist
	 * Open .bin file
	 */
	checkIsFileOpen.open(f_path, ios_base::out | ios_base::in);

	if (checkIsFileOpen.is_open()) {
		currFile.Open(1, f_path);
	} else {
		currFile.Open(0, f_path);
	}
	file_path = f_path;

  char path[100];
  sprintf(path, "%s.metadata", f_path);
  FILE *fptr = fopen(path, "wr");

  fwrite((int *)&f_type, 1, sizeof(f_type), fptr);

  SortInfo *si = new SortInfo();

  si->myOrder = new OrderMaker();
  memcpy(si->myOrder, ((SortInfo *)startup)->myOrder, sizeof(OrderMaker));

  si->runLength = ((SortInfo *)startup)->runLength;

  fwrite((int *)&si->runLength, 1, sizeof(int), fptr);
  fwrite((OrderMaker *)si->myOrder, 1, sizeof(OrderMaker), fptr);
  fclose(fptr);
	return 1;
}

void SortedFile::Load(Schema &f_schema, char *loadpath) {
#ifdef PRINT_2_B
	cout << "Begin Entry SortedFile::Load";
#endif
	/*currMode = WRITING;

	if (!bigQ) {
		inPipe = new Pipe(IN_OUT_PIPE_BUFF_SIZE);
		outPipe = new Pipe(IN_OUT_PIPE_BUFF_SIZE);
		bigQ = new BigQ(*inPipe, *outPipe, *sortOrder, runLen);
	}
*/
	/*
	 * Open .tbl file
	 */
	tblFile = fopen(loadpath, "rb");

	if (!tblFile) {
		cerr << "\nFailed to Open the file: %s" << loadpath;
		return;
	}

	currRecord = new (std::nothrow) Record;

	/*
	 * Read record(s) from .tbl file One at a time
	 * till EOF is reached
	 */
	while (currRecord->SuckNextRecord(&f_schema, tblFile)) {
		//inPipe->Insert(currRecord);
		 Add(*currRecord);
	}
	delete currRecord;

}

int SortedFile::Open(char *f_path) {
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
  si.myOrder = new OrderMaker();

	if (!fread(&f_type, 1, sizeof(fType), fptr)) {
		cerr << "\n f_type Read Error";
		exit(1);
	}
	fread((int *)&si.runLength, 1, sizeof(int), fptr);
	fread((OrderMaker *)si.myOrder, 1, sizeof(OrderMaker), fptr);
	/*
	 * Read from file and store the sortorder info and
	 * runLen in private members of SortedFile
	 */
	file_path = f_path;
  sortOrder = new OrderMaker();
  memcpy(sortOrder, si.myOrder, sizeof(OrderMaker));
	runLen = si.runLength;
	fclose(fptr);
	/*
	 * Create .bin file if doesn't exist
	 * Open .bin file
	 */
	checkIsFileOpen.open(f_path, ios_base::out | ios_base::in);

	if (checkIsFileOpen.is_open()) {
		currFile.Open(1, f_path);
	} else {
		currFile.Open(0, f_path);
	}


	currMode = READING;
#ifdef DEBUG
	cout<<"\n === Open currMode: READING ===";
#endif
	MoveFirst();
	return 1;
}

int SortedFile::Close() {
#ifdef PRINT_2_B
	cout << "Begin Entry SortedFile::Close";
#endif
	if (!outPipe || !inPipe) {
		return currFile.Close();
	}
	/*
	 * Close .bin file in case when we are just scanning
	 * through file OR
	 * close after successful creation of heap file

	if (!outPipe || !inPipe) {
		return currFile.Close();
	}


	 * If control falls through here,
	 * we are dealing with sorted file


	if (inPipe)
		inPipe->ShutDown();

	ComparisonEngine ceng;

	DBFile dbfile;
	char outfile[100];

	sprintf(outfile, "%s.bigq", file_path);
	dbfile.Create(outfile, heap, NULL);

	int i = 0;

	Record rec;
	Record *last = NULL, *prev = NULL;

	while (outPipe->Remove(&rec)) {
		dbfile.Add(*&rec);
		i++;
	}

	cout << "\n consumer: removed " << i << " recs from the pipe\n";

	cerr << "\n consumer: recs removed written out as heap DBFile at "
			<< outfile << endl;
	dbfile.Close();*/

	//write inQueue/inFlight records to disk
  if(WRITING == currMode){
	  cout << "SortedFile::Close Called in Writing Mode, proceeding with merge";
	 mergeInflghtRecs();
  }
	 //close the input/output pipes
  if(inPipe!=NULL) {
	  delete inPipe;
	  inPipe = NULL;
  }
  if(outPipe!=NULL) {
	  delete outPipe  ;
	  outPipe = NULL;
  }

//	currFile.Close();
	/*
	 * Remove XX.bin file and
	 * Rename XX.bin.bigq to XX.bin
	 */
//	remove(file_path);
//	rename(outfile, file_path);

	/*
	 * Need to be handled more gracefully
	 * the unwanted XX.bigq.metadata being created.
	 * One way is to call Append and AddPage APIs instead of
	 * dbfile.Add()
	 */
//	sprintf(outfile, "%s.metadata", outfile);
//	remove(outfile);
}

void SortedFile::MoveFirst() {

	/*
	 * Check if file really contain any records
	 */

//	cout << " Move First";
	if (currFile.GetLength() == 0) {
		//currPageIndex = 0;
		cout << "Bad operation , File Empty";
	} else {

		currPageIndex = 0;
		 cout << " Inside DB FIle Move First currPageIndex : "<<currPageIndex<<endl;
		currFile.MoveFirst();
		currFile.GetPage(&currPage, currPageIndex++);
		pageReadInProg = 1;
	}
}

void*
bigQueue1(void *vptr) {
	cout << "Creating BigQ constructor ";
	threadParams_t *inParams = (threadParams_t *) vptr;
	BigQ bq(*inParams->inPipe, *inParams->outPipe, *inParams->sortOrder,
			inParams->runLen);
}
void SortedFile::Add(Record &rec) {
//	cout << "Begin Entry SortedFile::Add for file :: "<< file_path << endl;
	counter++;
	//push record to inpur pipe
	//inPipe->Insert(&rec);
	// cout<<"\n#### Counter:" << counter << &rec <<"####";
	if (READING == currMode) {
		currMode = WRITING;

		if (flag == 0) {
			inPipe = new Pipe(IN_OUT_PIPE_BUFF_SIZE);
			outPipe = new Pipe(IN_OUT_PIPE_BUFF_SIZE);

			pthread_t thread1;

			threadParams_t *tp = new (std::nothrow) threadParams_t;

			/*
			 * use a container to pass arguments to worker thread
			 */
			tp->inPipe = inPipe;
			tp->outPipe = outPipe;
			tp->sortOrder = sortOrder;
			tp->runLen = runLen;
			//bigQ = new BigQ(*inPipe, *outPipe, *sortOrder, runLen);
			inPipe->Insert(&rec);
			pthread_create(&thread1, NULL, bigQueue1, (void *) tp);
			flag = 1;
		}
	} else if (WRITING == currMode) {
	  inPipe->Insert(&rec);
	} else {
	}
}

void SortedFile::AppendSequential(Record &appendme){


	  if(pageReadInProg==0) {
	    // currPageIndex = 460;
	    currFile.AddPage(&currPage, currFile.GetLength());
	    pageReadInProg = 1;
	  }


	  if(currFile.GetLength()>0) //existing page
	  {
	    currFile.GetPage(&currPage,currFile.GetLength()-2);
	    currPageIndex = currFile.GetLength()-2;
	  }
	  if(!currPage.Append(&appendme)) //full page
	  {
	    currPage.EmptyItOut();
	    currPage.Append(&appendme);
	    currPageIndex++;
	  }

	  currFile.AddPage(&currPage,currPageIndex);


}

void SortedFile::createMetaDataFile(char *fpath, fType f_type, OrderMaker *sortOrder,int runLen){
	char path[100];
		  sprintf(path, "%s.metadata", file_path);
		  FILE *fptr = fopen(path, "wr");

		  fwrite((int *)&f_type, 1, sizeof(f_type), fptr);

	//	  SortInfo *si = new SortInfo();

		//  si->myOrder = new OrderMaker();
	//	  memcpy(si->myOrder, ((SortInfo *)startup)->myOrder, sizeof(OrderMaker));

		//  si->runLength = ((SortInfo *)startup)->runLength;

		  fwrite((int *)&runLen, 1, sizeof(int), fptr);
		  fwrite((OrderMaker *)sortOrder, 1, sizeof(OrderMaker), fptr);
		  fclose(fptr);

}

void SortedFile::mergeInflghtRecs() {

//	cout << "\nin mergeInflights for file " << file_path << endl;
//	int pipeover=0, fileover = 0;

// shutdown input pipe
	inPipe->ShutDown();

//	cout << "\ninput pipe closed";
//	file.Close();

//empty the ouput pipe and do a two way merge

	// remove records from BigQ output pipe one at a time
	// at the same time u are removing records from the internal BigQ you scan the sorted files data in sorted order
	// use a standard two way merge algorithm u merge the two sets of data and write the result out to a new instance of the file Class

	Record *pipeRec;
	Record *fileRec;

	ComparisonEngine comp;


	time_t seconds;

	seconds = time(NULL);

//	cout << "\ntime in sseconds :" << seconds;

	struct timeval tval;
	gettimeofday(&tval, NULL);
	stringstream ss;
	ss << tval.tv_sec;
	ss << ".";
	ss << tval.tv_usec;

	string filename = "mergeFile" + ss.str();
	//string filename = "mergeFile" ;

//	cout << "\ntemp : File name :" << filename << endl;
//	cout << "\ntemp : file_path :" << file_path << endl;
//	cout << "\ntemp : file_path :" << file_path << endl;
//	cout << "\ntemp : file_path :" << file_path << endl;
//	cout << "\ntemp : file_path :" << file_path << endl;
	struct {OrderMaker *o; int l;} startup = {sortOrder, runLen};
	DBFile tmp;
	//		cout << "\n output to dbfile : " << strdup(filename.c_str()) << endl;
			tmp.Create (strdup(filename.c_str()), heap, &startup);

	//tmp.Open(strdup(filename.c_str()));
	//tmp.Open(0,filename);

//	Page *tmpPage = new Page();

	int fromPipe = 0, fromFile = 0;
	//currFile.MoveFirst();
	/*
	 DBFile dbfile;
	 dbfile.Open (file_path);
	 dbfile.MoveFirst ();*/

  //Record *backupCurrRecord = new Record();
  //backupCurrRecord->Copy(currRecord);
  //int    backupCurrPageIndex = currPageIndex;

	tmp.MoveFirst();
  MoveFirst();
	int k=0,l=0,m=0,n=0,o=0,p=0,q=0;
//	cout << " \nDB File Opened" << endl;
	int pipeYes=1,fileYes=1;

	pipeRec = new Record;
	fileRec = new Record;
	fromPipe = outPipe->Remove(pipeRec);
	fromFile = getRecordWithoutSort(*fileRec);
	while (1) {





	/*	if(pipeYes)
		fromPipe = outPipe->Remove(pipeRec);
		if(fileYes)
		fromFile = getRecordWithoutSort(*fileRec);	//  dbfile.GetNext(*fileRec);*/



		if(fromPipe)
			++k;
		if(fromFile)
			++l;

		if (fromPipe && fromFile) {
			++m;
	//		cout << "got records from pipe and file to be put in merge file"<<filename << endl;
			if (comp.Compare(pipeRec, fileRec, sortOrder) >= 0) {
				tmp.Add(*fileRec);
				fileRec = new Record;
				fromFile = getRecordWithoutSort(*fileRec);
				pipeYes=0;fileYes=1;
				++p;
			} else {
				tmp.Add(*pipeRec);
				pipeRec = new Record;
				fromPipe = outPipe->Remove(pipeRec);
				fileYes=0;pipeYes=1;
				++q;
			}

			continue;

		}/*
		 else{
		 break;
		 }
		 */
    else if (fromPipe && !fromFile) {
    	++n;
   // 	cout << "got records from pipe to be put in merge file"<<filename << endl;
			tmp.Add(*pipeRec);
			pipeRec = new Record;
			fromPipe = outPipe->Remove(pipeRec);
			fileYes=0;pipeYes=1;
		} else  if(!fromPipe && fromFile){
			++o;
		//	cout << "got records from file to be put in merge file"<<filename << endl;
			tmp.Add(*fileRec);
			fileRec = new Record;
			fromFile = getRecordWithoutSort(*fileRec);
			pipeYes=0;fileYes=1;
		} else {
			break;
    }
}
cout << "total records from pipe :" << k<<endl;
cout << "total records from file :" << l<<endl;
cout << "total times file and pipe called:" << m<<endl;
cout << "total times  pipe called:" << n<<endl;
cout << "total times file  called:" << o<<endl;
cout << "Total added from pipe :" <<q+n<<endl;
cout << "Total added from file :" <<p+o<<endl;
cout << "Total written to merge file :"<< p+q+n+o<<endl;
//	cout << "All records inserted in merge file proceeding with close and cleanup" << endl ;

	currFile.Close();
	tmp.Close();

//	if(tmp.GetLength() > 0){
	remove(file_path);

	rename((strdup(filename.c_str())), file_path);


	createMetaDataFile(file_path,sorted , sortOrder,runLen);
//	}
  //currRecord->Copy(backupCurrRecord);
  //currPageIndex = backupCurrPageIndex;
  Open(file_path);
  //currFile.GetPage(&currPage, currPageIndex-1);;
  //currPageIndex ++;
}

void SortedFile::toggleCurrMode() {

	if (READING == currMode) {
		currMode = WRITING;
	} else {
		currMode = READING;
	}

}


/*void SortedFile::createMetaFile() {
 char path[100];
 fType f_type;
 sprintf(path, "%s.metadata", file_path);


 ofstream metaFile;
 metaFile.open(path);
 metaFile << "Sorted"<< endl;
 metaFile << (sortInfo)->runLength<< endl;

 metaFile << ((sortInfo)->myOrder->numAtts)<<endl;


}*/

int SortedFile::GetNext(Record &fetchme) {
#if DEBUG
	cout<< " current page index :" << currPageIndex << endl;
	cout<< " current page length :" << currFile.GetLength() << endl;
#endif

	if (WRITING == currMode) {
		// change mode and merge the inflight
		toggleCurrMode();
	  mergeInflghtRecs();
	}
	if (pageReadInProg == 0) {
		// currPageIndex = 460;
//	  cout << "GetPage 1"<< currPageIndex << endl;
		currFile.GetPage(&currPage, currPageIndex);
		currPageIndex = currPageIndex + 1;
		pageReadInProg = 1;
	}

	//fetch page

	if (currPage.GetFirst(&fetchme)) {
		return 1;
	} else {

		if (!(currPageIndex > currFile.GetLength() - 2)) {//cout << "GetPage 2  page index :"<< currPageIndex << endl;
			currFile.GetPage(&currPage, currPageIndex++);
			pageReadInProg++;
			currPage.GetFirst(&fetchme);
			return 1;
		} else {
			return 0;
		}
	}
}

int SortedFile::getRecordWithSort(Record &fetchme, CNF &cnf, Record &literal) {


	while (1) {
		if (currPage.GetFirst(&fetchme) == 1) {
			ComparisonEngine ceng;
			//if (ceng.Compare(&literal, query, &fetchme, sortInfo->myOrder)
			if (ceng.Compare(&literal, query, &fetchme, sortOrder)
					== 0) {
				if (ceng.Compare(&fetchme, &literal, &cnf))
					return 1;
			}
			else return 0;
		}

		else {

			currPageIndex++;
			if (currPageIndex < currFile.GetLength() - 1)
				currFile.GetPage(&currPage, currPageIndex);
			else return 0;
		}
	}
}

int SortedFile::getRecordWithoutSort(Record &fetchme) {
    if (currPage.GetFirst(&fetchme) == 1) {
        return 1;
    }
    else {
      currPageIndex++;
      if (currPageIndex < currFile.GetLength() - 1) {
        currFile.GetPage(&currPage, currPageIndex);
        if(currPage.GetFirst(&fetchme) == 1)
          return 1;
        else
          return 0;
      }
      else return 0;
    }
}
int SortedFile::getRecordWithoutSort(Record &fetchme, CNF &cnf, Record &literal) {
  while (true) {
    if (currPage.GetFirst(&fetchme) == 1)
    {
      ComparisonEngine comp;
      if (comp.Compare(&fetchme, &literal, &cnf))
        return 1;
    } else
    {
      currPageIndex++;
      if (currPageIndex < currFile.GetLength() - 1)
        currFile.GetPage(&currPage, currPageIndex);
      else return 0;
    }
  }

}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {

	if (WRITING == currMode) {
		isQueryDoneAtleastOnce = 0;
		hasSortOrder = 1;
		toggleCurrMode();
		mergeInflghtRecs();

	}
  ComparisonEngine comp;
  cout << "\n FOUND : " << found;
  if(hasSortOrder) {
    if(!found) {
    /*
     * Binary search
     */
      cout << "\n before binary search call ";
      query = new OrderMaker();
			if (cnf.GetSortOrderAttsFromCNF(*sortOrder, *query, literalOrder) > 0) {
        cout <<"\n  ------------- Sort Order matches with search query ---------------";
				if (!BinarySearch(fetchme, cnf, literal)) {
          /* Binary search failed */
          return 0;
        }
        else {
          /* Binary search Success */
          found = 1;
          do {
            /* Compare against query orderMaker */
            if (comp.Compare(&fetchme, query, &literal, &literalOrder) > 0) {
              return 0;
            }
            /* Compare against cnf */
            if(comp.Compare (&fetchme, &literal, &cnf))
              return 1;
          }while(GetNext(fetchme));
        }

        cout <<"\n  ------------- I don't think it comes here ---------------";
        return 0;
      }
      
      else {
        /* Query Order and sort Order DO NOT match */
        /* Linear search */
        cout <<"\n  ------------- Sort Order DOES NOT MATCH with search query ---------------";
        found = 1;
        while(GetNext(fetchme)) {
          if(comp.Compare (&fetchme, &literal, &cnf))
            return 1;
        }
        return 0;
      }
    }// if(!found)

    else if(1 == found) {
        cout <<"\n  ------------- found = 1 ---------------";
      /*
       * Come here when we found out that query order doesn't match sortOrder
       */
      while(GetNext(fetchme)) {
        if(comp.Compare (&fetchme, &literal, &cnf))
          return 1;
      }
      return 0;
    }
  }
  else if(1 == found) {
    /*
     * Come here when we found out that query order doesn't match sortOrder
     */
    while(GetNext(fetchme)) {
      if(comp.Compare (&fetchme, &literal, &cnf))
        return 1;
    }
    return 0;
  }
  return 0;
}

int SortedFile::BinarySearch(Record& fetchme, CNF &cnf, Record &literal) {

  int low = 0;
  int mid = 0;
  int high = currFile.GetLength();
	ComparisonEngine comp;

  while(1) {
    if ((high - low) > 2) {
      mid = (low + high) / 2;

      //cout << "\n *** Looping ***" << " " << low+high;
      if ( mid % 2 != 0) {
        mid += 1;
      }
      currPage.EmptyItOut();
      currFile.GetPage(&currPage, mid);
      currPage.GetFirst(&fetchme);
      
      if (comp.Compare(&fetchme, query, &literal, &literalOrder) >= 0) {
        high = mid;
      }
      else {
        low = mid;
      }
    }
    else {
      cout << "\n *** Breaking Out ***";
      break;
    }
  }
  currPage.EmptyItOut();
  currFile.GetPage(&currPage, low);

  int ctr = 0;
  while(1) {
    if(!GetNext(fetchme)) {
      return 0;
    }
    else {
      if (comp.Compare(&fetchme, query, &literal, &literalOrder) == 0) {
        cout << "\n *** Return from Binary here***" << " " << ++ctr;
        return 1;
      }
      else
        continue;
    }
  }
}
