#include "BigQ.h"
#include "vector"
#include "algorithm"
#include "exception"
#include <sys/time.h>
#include <sstream>

/*
 * global declarations
 */
OrderMaker *g_sortOrder;
int g_runCount = 0;

File g_file;
char *g_filePath;

std::vector<recOnVector*> recSortVectCurrent;

std::vector<Page*> pageSortVect;

std::vector<recOnVector*> recSortVect;
std::vector<Page *> PageSortVect;
std::vector<int> pageIndexVect;
std::vector<int> pageCountPerRunVect;

/*
 * Constructor
 */
recOnVector::recOnVector() {
	currRecord =new Record();
	currPageNumber = currRunNumber = 0;
}



void
moveRunToPages(threadParams_t *inParams)
{
  Record *currRecord = NULL;
  Page currPage;

  int pageAppendStatus = 0;
  int pageNumber = 0;

  cout << "g_file.GetLength() :" << g_file.GetLength() << endl;

  pageNumber = g_file.GetLength();


  pageIndexVect.push_back(pageNumber);

  //   recVector = new recOnVector;
int x=0;
  for (int i = 0; i < recSortVect.size(); i++) {
++x;
    currRecord = recSortVect[i]->currRecord;

    /*
     * Append record to current page
     */

    pageAppendStatus = currPage.Append(currRecord);

    if (!pageAppendStatus) {

      /*
       * If Append could not succeed, write page to file
       */

      g_file.AddPage(&currPage, pageNumber);

      pageAppendStatus = 1;
      /*
       * Flush the page to re-use it to store further records
       */
      currPage.EmptyItOut();
      pageNumber++;

      currPage.Append(currRecord);
    }

//cout << " total records : "<<x<<endl;
  } // End of for loop
  /*
   * Last page being written to file may not
   * be full
   */
  g_file.AddPage(&currPage, pageNumber);


  pageNumber++;
}

bool
fptrSortSingleRun(const recOnVector *left,
                  const recOnVector *right)
{
  ComparisonEngine comp;
  int retVal = 0;

  Record *r1 = left->currRecord;
  Record *r2 = right->currRecord;

  retVal = comp.Compare(r1, r2, g_sortOrder);

  if (retVal < 0) {
    return 1;
  } else {
    return 0;
  }

}

bool
mergeSortSingleRun(const recOnVector *left,
                   const recOnVector *right)
{
  ComparisonEngine comp;
  int retVal = 0;

  Record *r1 = left->currRecord;
  Record *r2 = right->currRecord;

  retVal = comp.Compare(r1, r2, g_sortOrder);

  if (retVal > 0) {
    return 0;
  } else {
    return 1;
  }

}

bool
fptrHeapSort(const recOnVector *left,
             const recOnVector *right)
{
  ComparisonEngine compareengine;
  Record *record1 = (left->currRecord);
  Record *record2 = (right->currRecord);
  int compresult = compareengine.Compare(record1,record2,g_sortOrder);
  if(compresult < 0)
    return false;
  else
    return true;
}

void
merge_pages(threadParams_t *inParams)
{int y=0;

  g_file.Open(1, g_filePath);

  cout <<"length of the file :"<<g_file.GetLength();

  Page* currPage = NULL;
  recOnVector *recVector_current;
  recOnVector *recVector_next;

  /*
   * start with the first page in each run.
   * Compare records from each run to give out the smallest
   * on Output Pipe.
   */
  pageSortVect.clear();
  for(int i = 0; i<pageIndexVect.size(); i++)
  {

    /*
     * start with first page in each run index of which
     * are stored in pageIndexVect
     */
    currPage = new Page();
    g_file.GetPage(currPage,pageIndexVect[i]);
    pageSortVect.push_back(currPage);

    /*
     * Read first record from each page in each run
     */
    Record *newrecord = new Record();
    pageSortVect[i]->GetFirst(newrecord);
    cout << "newrecord getbits :"<< newrecord->GetBits() << endl;

    /*
     * Stage-1
     * Form one vector across the first record in each run
     * to find the minimum elements
     */
    recVector_current = new recOnVector();
    recVector_current->currRecord = newrecord;
    recVector_current->currRunNumber = i;
    recVector_current->currPageNumber = pageIndexVect[i];


    recSortVect.push_back(recVector_current);
  }

  cout<< "IN merge phase2";
  /*
   * Every iteration of below loop will give out
   * smallest element from shriking record set
   */
  while(!recSortVect.empty())
  {
    int out_run = 0;
    int out_page = 0;


    /*
     * Heapify the vector in Stage-1 to find the minimum
     * so that we can start putting it on Output pipe
     */

 //   cout << "before paint"<<endl;
//    cout << "recSortVect.size() ::"<<recSortVect.size()<<endl;
                for (int i=0; i<recSortVect.size(); i++) {

 //               cout <<  recSortVect[i]->currRecord->GetBits() << endl;
 //               cout <<  recSortVect[i]->currRecord->GetBits() << endl;
 //               cout <<  recSortVect[i]->currRecord->GetBits();
 //               cout <<  recSortVect[i]->currRecord->GetBits();
 //               cout <<  recSortVect[i]->currRecord->GetBits();
                }

    std::make_heap(recSortVect.begin(),
                   recSortVect.end(),
                   fptrHeapSort);

    recVector_current = new recOnVector();
    recVector_current = recSortVect.front(); /* <--- minimum element */
    out_run = recVector_current->currRunNumber;
    out_page = recVector_current->currPageNumber;
    /*
     * Remove smallest element from heap
     */
    std::pop_heap(recSortVect.begin(),recSortVect.end());
    recSortVect.pop_back();

    /*
     * Output smallest element to Pipe
     */
++y;
//cout << "writtent to out pipe :"<<y<<endl;

    inParams->outPipe->Insert(recVector_current->currRecord);
    /*
     * Fill the empty space formed after removing the smallest element
     * by NEXT element from SAME RUN (althrough diff page on SAME RUN).
     * Create new enrty on vector for new/comin element
     */
    recVector_next = new recOnVector();
    recVector_next->currRunNumber = out_run;
    recVector_next->currPageNumber = out_page;

    /*
     * Check if you can get it on same page.
     * If not move on to next page in same Run
     */
    if(pageSortVect[out_run]->GetFirst(recVector_next->currRecord)) {
      recSortVect.push_back(recVector_next);
    }
    else {

      /* check withing available pages Only */
      if(out_page+2 < g_file.GetLength()){

        if(out_page< pageIndexVect[out_run+1]-1) {
          /*
           * jump to next page in SAME run
           */
          g_file.GetPage(currPage,out_page+1);
          pageSortVect[out_run] = currPage;
          /*
           * Start with first record in new page and keep filling the
           * hole formed
           */
          if(pageSortVect[out_run]->GetFirst(recVector_next->currRecord)) {
            recVector_next->currPageNumber = out_page+1;
            recSortVect.push_back(recVector_next);
          }
          currPage = new Page();
        }
      } /* end of page boundary 'if' */

    }
  }

  g_file.Close();

}

void*
bigQueue(void *vptr) {

	pageIndexVect.clear();
	 recSortVect.clear();

	threadParams_t *inParams = (threadParams_t *) vptr;

  cout <<"\n ********** Thread bigQueue ***********";
  cout <<"\n ********** Thread bigQueue ***********";
	Record fetchedRecord;
	Page tmpBufferPage;
	recOnVector *tmpRecordVector;

	int numPages = 0;
	bool record_present = 1;
	bool appendStatus = 0;

	g_file.Open(0, g_filePath);
int k=0;
	while (record_present) {

		while (numPages <= inParams->runLen) {
			/*
			 * Fetch record(s) from input pipe one by one
			 * and add it to a page/runs
			 */
			if (inParams->inPipe->Remove(&fetchedRecord)) {
			//	cout << "num of records : "<<++k<<endl;
				/*
				 * Create a copy of a record to store it in vector,
				 * because 'fetchedRecord' is local variable
				 *
				 * TODO: Consider using non-pointer for copyRecord variable,
				 * so that it reduces risk of memory leak - DONE
				 */
				/*
				 * Create dummy object to store on vector with
				 * metadata (record, run number and page number)
				 */
				tmpRecordVector = new (std::nothrow) recOnVector;
				tmpRecordVector->currRecord = new Record;

				tmpRecordVector->currRecord->Copy(&fetchedRecord);
				tmpRecordVector->currRunNumber = g_runCount;


				/*
				 * Push the record on vector at the end
				 */
				recSortVect.push_back(tmpRecordVector);
				/*
				 * try adding the record to Page/run
				 */

				appendStatus = tmpBufferPage.Append(&fetchedRecord);

				/*
				 * if page has no space to accommodate this record
				 * Clean it and reuse it. Increment page count.
				 */
				if (0 == appendStatus) {

					numPages++;
					tmpBufferPage.EmptyItOut();
					tmpBufferPage.Append(&fetchedRecord);



				}
			} else {
				/*
				 * Stop removing records from input pipe.
				 * No more records left there
				 */
				record_present = 0;
			//	cout << "Exiting Bq";
				break;
			}
		}/* End of while(numPages <= inParams->runLen) */

		cout << "numPages :"<<numPages<<endl;
		pageCountPerRunVect.push_back(numPages);

		/* Reset the number of pages per run */
		numPages = 0;

  cout <<"\n ********** recSortVector Size: " << recSortVect.size() <<"***********";



		/*
     * Sort each individual run
     */
		std::sort(recSortVect.begin(),
              recSortVect.end(),
              fptrSortSingleRun);

		/*
     * Convert each run to pages
     */
		moveRunToPages(inParams);



		/*
		 * TODO: Free currRecord buffer in each entry in vector.
		 * This was allocated before each record added to vector
		 */
		recSortVect.clear();
		g_runCount++;
	} /* End of while(record_present) */

	/*
	 * close temporary file
	 */
	g_file.Close();



	merge_pages(inParams);

	remove(g_filePath);
  cout <<"\n ********** Thread bigQueue Over ***********";
}


BigQ::BigQ(Pipe &in,
           Pipe &out,
           OrderMaker &sortorder,
           int runlen) {

	// read data from in pipe sort them into runlen pages

	threadParams_t *tp = new (std::nothrow) threadParams_t;

	/*
	 * use a container to pass arguments to worker thread
	 */

	struct timeval tval;
		gettimeofday(&tval, NULL);
		stringstream ss;
		ss << tval.tv_sec;
		ss << ".";
		ss << tval.tv_usec;

		string filename = "partial" + ss.str();

	g_filePath = strdup(filename.c_str());

	tp->inPipe = &in;
	tp->outPipe = &out;
	tp->sortOrder = &sortorder;
	tp->runLen = runlen;
	g_sortOrder = &sortorder;
	/*
	 * Create worker Thread for sorting purpose
	 */
	pthread_t thread3;
  cout <<"\n ********** Creating bigQueue Thread ***********";
	pthread_create(&thread3, NULL, bigQueue, (void *) tp);

	pthread_join(thread3, NULL);

	// construct priority queue over sorted runs and dump sorted data
	// into the out pipe

	// finally shut down the out pipe
	for(int l=0;l<100;l++);
	out.ShutDown();
  cout <<"\n ********** Thread FINISHED ***********";
}

BigQ::~BigQ() {
}
