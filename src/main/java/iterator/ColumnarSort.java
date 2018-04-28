package iterator;

import bitmap.*;
import columnar.ColumnarFile;
import columnar.ColumnarFileDoesExistsException;
import columnar.ColumnarFilePinPageException;
import columnar.ColumnarFileUnpinPageException;
import diskmgr.DiskMgrException;
import diskmgr.Page;
import global.*;
import heap.*;
import heap.InvalidTupleSizeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

public class ColumnarSort implements GlobalConst {
    private Heapfile sortedHeapFileNames[];
    private Heapfile heapfiles[];
    private int coloumnNo;
    private int numOfColumn;
    private String columnarFileName;
    private int numruns;
    AttrType sortedColumnType;
    String order;

    // Algo
    // create a new heap file.
    // loop around the pages of old heap file.
    // apply in-memory sort for every page
    // write the page into the new heap file.
    // I will now have the number of heap files.
    // run a loop dividing by 2
    // run until divide by 2 results into 1 file.

    public ColumnarSort(String columnarFileName, int columnNo, String order)
            throws DiskMgrException, ColumnarFileDoesExistsException, ColumnarFilePinPageException, HFException,
            HFBufMgrException, HFDiskMgrException, ColumnarFileUnpinPageException, PinPageException,
            AddFileEntryException, UnpinPageException, ConstructPageException, GetFileEntryException, IOException,
            InvalidTupleSizeException, Exception {
        ColumnarFile columnarFile = new ColumnarFile(columnarFileName);
        this.coloumnNo = columnNo;
        this.order = order;
        numOfColumn = columnarFile.getNumColumns();
        sortedHeapFileNames = new Heapfile[numOfColumn];
        // create the heap files for the sorted records

        this.columnarFileName = columnarFileName;
        // sortedHeapFileNames = new Heapfile(columnsFileName);
        // for (int i = 0; i < numOfColumn; i++) {
        //
        // sortedHeapFileNames[i] = new Heapfile(columnsFileName);
        //
        // }
        // now insert the sorted pages in the heap files.
        // individual pages would be sorted, not the whole file
        int numDataPages = init(columnarFile);
        if (numDataPages == 1) {
            copySortedFile();
            return;
        }
        // System.out.println(numDataPages);
        // Real work begins
        mergesort(numDataPages);

    }

    private void copySortedFile()
            throws HFException, HFBufMgrException, HFDiskMgrException, IOException, DiskMgrException {
        Heapfile firstLevel = new Heapfile(columnarFileName + "s0r" + coloumnNo + order);
        SystemDefs.JavabaseDB.updateFileEntry(columnarFileName + "s0r" + coloumnNo + order, columnarFileName + ".s" + coloumnNo + order, firstLevel._firstDirPageId);
    }

    private void mergesort(int numDataPages) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
            InvalidSlotNumberException, InvalidTupleSizeException, Exception {
        /*
         * Calculate how many runs do we need run a loop on them for every run
         * initialize an array of heap file. name them into sort+ number of run+ column
         * number the last run will have name as tablename.column.sort every run will
         * take input from the previous run according to the run scan that many pages
         * delete all the heap file beyond that.
         *
         *
         *
         */
        HFPage dirPage = new HFPage();
        RID curDataPageRid = new RID();
        Tuple tuple = new Tuple();
        DataPageInfo info = new DataPageInfo();
        HFPage curDataPage = new HFPage();
        HFPage curDataPage2 = new HFPage();
        PageId curDataPageId = new PageId();
        PageId curDataPageId2 = new PageId();
        PageId curDirPageId = new PageId();
        PageId nextPageId1 = new PageId();
        PageId nextPageId2 = new PageId();
        PageId nextdirePageId = new PageId();
        RID firstDataPageRID = new RID();

        // num of runs needed
        numruns = ((int) Math.ceil((Math.log(numDataPages) / Math.log(2))));
        // loop for the runs
        for (int i = 0; i < numruns; i++) {
            Heapfile[] heapfile = new Heapfile[numOfColumn];
            // if(i != numruns-1)
            int filenum = i + 1;
            if (i != numruns - 1)
                heapfile[coloumnNo] = new Heapfile(columnarFileName + "s" + filenum + "r" + coloumnNo + order);
            else {
                heapfile[coloumnNo] = new Heapfile(columnarFileName + ".s" + coloumnNo + order);

            }

            // else
            // heapfile[coloumnNo] = new Heapfile(columnarFileName + coloumnNo + ".s");
            // get the last heapfile
            Heapfile lastheapfile = new Heapfile(columnarFileName + "s" + (i) + "r" + coloumnNo + order);
            // get the first page of the heapfile
            // first directory page
            curDirPageId = new PageId(lastheapfile.get_firstDirPageId().pid);
            pinPage(curDirPageId, dirPage, false);

            int nextsecondposition = (int) Math.pow(2, i) - 1;
            boolean found1 = true;
            boolean start = true;
            int k = 0;
            while (curDirPageId.pid != INVALID_PAGE) {
                curDataPageRid = dirPage.firstRecord();
                while (curDataPageRid != null) {
                    if ((curDataPageId2.pid == INVALID_PAGE || curDataPageId2.pid == 0) && found1 && start) {
                        try {
                            tuple = dirPage.getRecord(curDataPageRid);
                        } catch (InvalidSlotNumberException e)// check error! return false(done)
                        {
                            return;
                        }
                        info = new DataPageInfo(tuple);
                        try {
                            curDataPageId = info.pageId;
                            start = false;
                            curDataPageId2.pid = INVALID_PAGE;

                        } catch (Exception e) {
                            unpinPage(curDirPageId, false/* undirty */);
                            throw e;
                        }

                    } else if ((curDataPageId2.pid != INVALID_PAGE || curDataPageId2.pid != 0) && found1) {
                        while (k <= nextsecondposition) {
                            curDataPageRid = dirPage.nextRecord(curDataPageRid);
                            k++;
                            if (curDataPageRid == null) {
                                break;
                            }

                        }
                        if (curDataPageRid != null) {
                            k = 0;
                            try {
                                tuple = dirPage.getRecord(curDataPageRid);
                            } catch (InvalidSlotNumberException e)// check error! return false(done)
                            {
                                return;
                            }
                            info = new DataPageInfo(tuple);
                            try {
                                curDataPageId = info.pageId;
                                curDataPageId2.pid = INVALID_PAGE;

                            } catch (Exception e) {
                                unpinPage(curDirPageId, false/* undirty */);
                                throw e;
                            }
                        } else {
                            break;
                        }

                    }

                    if (curDataPageId.pid != INVALID_PAGE && curDataPageRid != null) {
                        while (k <= nextsecondposition) {
                            curDataPageRid = dirPage.nextRecord(curDataPageRid);
                            k++;

                            if (curDataPageRid == null) {
                                found1 = false;
                                break;
                            }

                        }
                        if (curDataPageRid != null) {
                            found1 = true;
                            k = 0;
                            try {
                                tuple = dirPage.getRecord(curDataPageRid);
                            } catch (InvalidSlotNumberException e)// check error! return false(done)
                            {
                                return;
                            }
                            info = new DataPageInfo(tuple);
                            try {
                                curDataPageId2 = info.pageId;

                            } catch (Exception e) {
                                unpinPage(curDirPageId, false/* undirty */);
                                throw e;
                            }
                        }

                    }
                    if (curDataPageId.pid != INVALID_PAGE && curDataPageId2.pid != INVALID_PAGE) {
                        mergeRuns(curDataPageId, curDataPageId2, i);
                        curDataPageId.pid = INVALID_PAGE;
                    }

                }
                nextdirePageId = dirPage.getNextPage();

                try {
                    unpinPage(dirPage.getCurPage(), false /* undirty */);
                } catch (Exception e) {
                    throw new HFException(e, "heapfile,_find,unpinpage failed");
                }

                curDirPageId.pid = nextdirePageId.pid;
                if (curDirPageId.pid != INVALID_PAGE) {

                    pinPage(curDirPageId, dirPage, false/* Rdisk */);
                } else {
                    curDataPageId2.pid = INVALID_PAGE;
                }

            }
            if (curDataPageId2.pid == INVALID_PAGE && curDataPageId.pid != INVALID_PAGE) {

                mergeRuns(curDataPageId, curDataPageId2, i);
            }

            if (i >= 0) {

                Heapfile heapTodistroy = new Heapfile(columnarFileName + "s" + (i) + "r" + coloumnNo + order);
                heapTodistroy.deleteFile();
            }
        }


    }

    private int order() {
        if (order.equalsIgnoreCase("DSC")) {
            return -1;

        } else if (order.equalsIgnoreCase("ASC")) {
            return 1;
        } else {
            return 0;
        }

    }

    private void mergeRuns(PageId curPage, PageId curPage2, int i)
            throws HFBufMgrException, IOException, InvalidSlotNumberException, HFException, HFDiskMgrException,
            InvalidTupleSizeException, SpaceNotAvailableException {
        /*
         * pin first page pin second page while take first record from first page take
         * first record from second page compare them if first is smaller insert first =
         * first.next if second is smaller second.next if both are equal insert both if
         * first record return null then firstpage =firstpage.next if scond record
         * return null then second = second ka next above two steps uptill i if all
         * records of first exhausted then run sequential scan on second and insert all
         * its remaining tuple and vice versa
         *
         */
        Heapfile heapfile[] = new Heapfile[numOfColumn];
        int filenum = i + 1;
        if (i != numruns - 1)
            heapfile[coloumnNo] = new Heapfile(columnarFileName + "s" + filenum + "r" + coloumnNo + order);
        else
            heapfile[coloumnNo] = new Heapfile(columnarFileName + ".s" + coloumnNo + order);

        int nextPageLimit = (int) Math.pow(2, i) - 1;
        HFPage list1 = new HFPage();
        HFPage list2 = new HFPage();
        PageId nextPage1 = new PageId();
        PageId nextPage2 = new PageId();
        RID record1 = null;
        RID record2 = null;
        // it is for int latr will change
        ValueClass val1 = null;
        ValueClass val2 = null;
        int counter1 = 0;
        int counter2 = 0;
        if (curPage.pid != INVALID_PAGE)
            pinPage(curPage, list1, false);
        if (curPage2.pid != INVALID_PAGE)
            pinPage(curPage2, list2, false);
        record1 = list1.firstRecord();
        record2 = list2.firstRecord();

        while (record1 != null && record2 != null) {
            if (sortedColumnType.getAttrType() == 1) {
                val1 = (IntegerValue) getValuefromSortByte(list1.getRecord(record1).getTupleByteArray());
                val2 = (IntegerValue) getValuefromSortByte(list2.getRecord(record2).getTupleByteArray());
            } else if (sortedColumnType.getAttrType() == 0) {
                val1 = (StringValue) getValuefromSortByte(list1.getRecord(record1).getTupleByteArray());
                val2 = (StringValue) getValuefromSortByte(list2.getRecord(record2).getTupleByteArray());
            }
            int res = val1.cmp(val2) * order();
            if (res < 0) {
                heapfile[coloumnNo].insertRecord(list1.getRecord(record1).getTupleByteArray());
                record1 = list1.nextRecord(record1);
                if (record1 == null && counter1 < nextPageLimit) {
                    nextPage1 = list1.getNextPage();
                    try {
                        unpinPage(curPage, false /* undirty */);
                    } catch (Exception e) {
                        throw new HFException(e, "heapfile,_find,unpinpage failed");
                    }

                    curPage.pid = nextPage1.pid;
                    if (curPage.pid != INVALID_PAGE) {
                        counter1++;
                        pinPage(curPage, list1, false/* Rdisk */);
                        record1 = list1.firstRecord();
                    }

                } else if (record1 == null && counter1 == nextPageLimit) {
                    try {
                        unpinPage(curPage, false /* undirty */);
                    } catch (Exception e) {
                        throw new HFException(e, "heapfile,_find,unpinpage failed");
                    }

                }
            } else if (res > 0) {
                heapfile[coloumnNo].insertRecord(list2.getRecord(record2).getTupleByteArray());
                record2 = list2.nextRecord(record2);
                if (record2 == null && counter2 < nextPageLimit) {
                    nextPage2 = list2.getNextPage();
                    try {
                        unpinPage(curPage2, false /* undirty */);
                    } catch (Exception e) {
                        throw new HFException(e, "heapfile,_find unpinpage failed");
                    }

                    curPage2.pid = nextPage2.pid;
                    if (curPage2.pid != INVALID_PAGE) {
                        counter2++;
                        pinPage(curPage2, list2, false);
                        record2 = list2.firstRecord();
                    }

                } else if (record2 == null && counter2 == nextPageLimit) {
                    try {
                        unpinPage(curPage2, true /* undirty */);
                    } catch (Exception e) {
                        throw new HFException(e, "heapfile,_find unpinpage failed");
                    }
                }

            } else {
                heapfile[coloumnNo].insertRecord(list1.getRecord(record1).getTupleByteArray());
                heapfile[coloumnNo].insertRecord(list2.getRecord(record2).getTupleByteArray());
                record1 = list1.nextRecord(record1);
                record2 = list2.nextRecord(record2);
                if (record1 == null && counter1 < nextPageLimit) {
                    nextPage1 = list1.getNextPage();
                    try {
                        unpinPage(curPage, false /* undirty */);
                    } catch (Exception e) {
                        throw new HFException(e, "heapfile,_find,unpinpage failed");
                    }

                    curPage.pid = nextPage1.pid;
                    if (curPage.pid != INVALID_PAGE) {
                        counter1++;
                        pinPage(curPage, list1, false/* Rdisk */);
                        record1 = list1.firstRecord();

                    }

                } else if (record1 == null && counter1 == nextPageLimit) {
                    try {
                        unpinPage(curPage, false /* undirty */);
                    } catch (Exception e) {
                        throw new HFException(e, "heapfile,_find,unpinpage failed");
                    }

                }
                if (record2 == null && counter2 < nextPageLimit) {
                    nextPage2 = list2.getNextPage();
                    try {
                        unpinPage(curPage2, false /* undirty */);
                    } catch (Exception e) {
                        throw new HFException(e, "heapfile,_find unpinpage failed");
                    }

                    curPage2.pid = nextPage2.pid;
                    if (curPage2.pid != INVALID_PAGE) {
                        counter2++;
                        pinPage(curPage2, list2, false);
                        record2 = list2.firstRecord();
                    }

                } else if (record2 == null && counter2 == nextPageLimit) {
                    try {
                        unpinPage(curPage2, false /* undirty */);
                    } catch (Exception e) {
                        throw new HFException(e, "heapfile,_find unpinpage failed");
                    }
                }

            }

        }
        while (record1 == null && record2 != null) {
            heapfile[coloumnNo].insertRecord(list2.getRecord(record2).getTupleByteArray());
            record2 = list2.nextRecord(record2);
            if (record2 == null && counter2 < nextPageLimit) {
                nextPage2 = list1.getNextPage();
                try {
                    unpinPage(curPage2, false /* undirty */);
                } catch (Exception e) {
                    throw new HFException(e, "heapfile,_find unpinpage failed");
                }

                curPage2.pid = nextPage2.pid;
                if (curPage2.pid != INVALID_PAGE) {
                    counter2++;
                    pinPage(curPage2, list2, false);
                    record2 = list2.firstRecord();
                }

            } else if (record2 == null && counter2 == nextPageLimit) {
                try {
                    unpinPage(curPage2, false /* undirty */);
                } catch (Exception e) {
                    throw new HFException(e, "heapfile,_find unpinpage failed");
                }
            }
        }
        while (record1 != null && record2 == null) {
            heapfile[coloumnNo].insertRecord(list1.getRecord(record1).getTupleByteArray());
            record1 = list1.nextRecord(record1);
            if (record1 == null && counter1 < nextPageLimit) {
                nextPage1 = list1.getNextPage();
                try {
                    unpinPage(curPage, false /* undirty */);
                } catch (Exception e) {
                    throw new HFException(e, "heapfile,_find,unpinpage failed");
                }

                curPage.pid = nextPage1.pid;
                if (curPage.pid != INVALID_PAGE) {
                    counter1++;
                    pinPage(curPage, list1, false/* Rdisk */);
                    record1 = list1.firstRecord();
                }

            } else if (record1 == null && counter1 == nextPageLimit) {
                try {
                    unpinPage(curPage, false /* undirty */);
                } catch (Exception e) {
                    throw new HFException(e, "heapfile,_find,unpinpage failed");
                }

            }

        }

    }

    /*
     *
     * function to get the first list of sorted page
     */

    private int init(ColumnarFile columnarFile)
            throws InvalidTupleSizeException, IOException, HFBufMgrException, Exception {
        Heapfile heapfile_0 = new Heapfile(columnarFileName + "s0r" + coloumnNo + order);
        Heapfile temp = new Heapfile("temp.sort");
        heapfiles = columnarFile.getHeapFileNames();
        // can throw error if columnNo is greater than the heapfiles size
        sortedColumnType = columnarFile.getColumnInfo(coloumnNo);
        PageId currentDirPageId = new PageId(heapfiles[coloumnNo].get_firstDirPageId().pid);
        HFPage currentDirPage = new HFPage();
        HFPage currentDataPage = new HFPage();
        PageId nextDirPageId = new PageId();
        pinPage(currentDirPageId, currentDirPage, false/* read disk */);
        ArrayList<SortInfo> sortingList = new ArrayList<SortInfo>();
        Tuple atuple = new Tuple();
        RID currentDataPageRid = new RID();
        RID record = new RID();
        int count = 0;
        int totalPageCount = 0;
        int size = sortedColumnType.getSize();
        // loop around the directory pages
        while (currentDirPageId.pid != INVALID_PAGE) {
            // loop around the data pages of directory pages
            for (currentDataPageRid = currentDirPage
                    .firstRecord(); currentDataPageRid != null; currentDataPageRid = currentDirPage
                    .nextRecord(currentDataPageRid)) {

                try {
                    atuple = currentDirPage.getRecord(currentDataPageRid);
                } catch (InvalidSlotNumberException e)// check error! return false(done)
                {
                    return 0;
                }
                DataPageInfo dpinfo = new DataPageInfo(atuple);
                try {
                    // pin the datapage
                    pinPage(dpinfo.pageId, currentDataPage, false/* Rddisk */);
                    // check error;need unpin currentDirPage
                } catch (Exception e) {
                    unpinPage(currentDirPageId, false/* undirty */);
                    throw e;
                }

                // loop around the records
                for (record = currentDataPage.firstRecord(); record != null; record = currentDataPage
                        .nextRecord(record)) {
                    temp.insertRecord(
                            getBytefromSortInfo(currentDataPage.getRecord(record).getTupleByteArray(), count));
                    count++;
                }
                unpinPage(dpinfo.pageId, false /* undirty */);
            }
            nextDirPageId = currentDirPage.getNextPage();
            try {
                unpinPage(currentDirPageId, false /* undirty */);
            } catch (Exception e) {
                throw new HFException(e, "heapfile,_find,unpinpage failed");
            }

            currentDirPageId.pid = nextDirPageId.pid;
            if (currentDirPageId.pid != INVALID_PAGE) {
                pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);
            }
        }
        currentDirPageId = new PageId(temp.get_firstDirPageId().pid);
        currentDirPage = new HFPage();
        currentDataPage = new HFPage();
        pinPage(currentDirPageId, currentDirPage, false/* read disk */);
        while (currentDirPageId.pid != INVALID_PAGE) {
            // loop around the data pages of directory pages
            for (currentDataPageRid = currentDirPage
                    .firstRecord(); currentDataPageRid != null; currentDataPageRid = currentDirPage
                    .nextRecord(currentDataPageRid)) {
                totalPageCount++;
                try {
                    atuple = currentDirPage.getRecord(currentDataPageRid);
                } catch (InvalidSlotNumberException e)// check error! return false(done)
                {
                    return 0;
                }
                DataPageInfo dpinfo = new DataPageInfo(atuple);
                try {
                    // pin the datapage
                    pinPage(dpinfo.pageId, currentDataPage, false/* Rddisk */);
                    // check error;need unpin currentDirPage
                } catch (Exception e) {
                    unpinPage(currentDirPageId, false/* undirty */);
                    throw e;
                }
                sortingList.clear();
                for (record = currentDataPage.firstRecord(); record != null; record = currentDataPage
                        .nextRecord(record)) {
                    if (sortedColumnType.getAttrType() == 0) {
                        sortingList.add(new SortInfo(
                                (StringValue) getValuefromSortByte(
                                        currentDataPage.getRecord(record).getTupleByteArray()),
                                getPositionfromSortByte(currentDataPage.getRecord(record).getTupleByteArray())));
                    } else if (sortedColumnType.getAttrType() == 1) {
                        sortingList.add(new SortInfo(
                                new IntegerValue(
                                        Convert.getIntValue(0, currentDataPage.getRecord(record).getTupleByteArray())),
                                getPositionfromSortByte(currentDataPage.getRecord(record).getTupleByteArray())));
                    }

                }

                // unpin page immediately
                unpinPage(dpinfo.pageId, false);
                if (order.equalsIgnoreCase("ASC")) {
                    // sort a page using comparator
                    sortingList.sort(new Comparator<SortInfo>() {
                        public int compare(SortInfo o1, SortInfo o2) {
                            // TODO Auto-generated method stub
                            return o1.compareTo(o2);
                        }
                    });
                } else if (order.equalsIgnoreCase("DSC")) {
                    sortingList.sort(new Comparator<SortInfo>() {
                        public int compare(SortInfo o1, SortInfo o2) {
                            // TODO Auto-generated method stub
                            return o2.compareTo(o1);
                        }
                    });

                }
                // Inserting to new heap file
                for (SortInfo val : sortingList) {
                    byte[] recPtr = null;
                    if (sortedColumnType.getAttrType() == 0) {
                        recPtr = new byte[size];
                        Convert.setStringValue((String) val.getVal().getValue(), 0, recPtr);
                    } else if (sortedColumnType.getAttrType() == 1) {
                        recPtr = new byte[4];
                        Convert.setIntValue((Integer) val.getVal().getValue(), 0, recPtr);
                    }
                    heapfile_0.insertRecord(getBytefromSortInfo(recPtr, val.position));

                }
                // sortOtherHeapFilesInit(dirCount, datapageCount, size, sortingList);

            }
            nextDirPageId = currentDirPage.getNextPage();
            try {
                unpinPage(currentDirPageId, false /* undirty */);
            } catch (Exception e) {
                throw new HFException(e, "heapfile,_find,unpinpage failed");
            }

            currentDirPageId.pid = nextDirPageId.pid;
            if (currentDirPageId.pid != INVALID_PAGE) {

                pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);
            }
        }
        temp.deleteFile();

        return totalPageCount;
    }

    byte[] getBytefromSortInfo(byte[] record, int position) throws IOException {
        byte[] pos = new byte[4];
        Convert.setIntValue(position, 0, pos);
        byte[] sortPos = new byte[sortedColumnType.getSize() + 4];
        System.arraycopy(record, 0, sortPos, 0, sortedColumnType.getSize());
        System.arraycopy(pos, 0, sortPos, sortedColumnType.getSize(), 4);

        return sortPos;
    }

    ValueClass getValuefromSortByte(byte[] record) throws IOException {
        int size = sortedColumnType.getSize();
        ValueClass val = null;
        if (sortedColumnType.getAttrType() == 1) {
            val = new IntegerValue(Convert.getIntValue(0, record));
        } else if (sortedColumnType.getAttrType() == 0) {
            val = new StringValue(Convert.getStringValue(0, record, size));

        }
        return val;
    }

    int getPositionfromSortByte(byte[] record) throws IOException {
        int size = sortedColumnType.getSize();
        int pos = -1;

        pos = Convert.getIntValue(size, record);

        return pos;
    }

    /**
     * short cut to access the pinPage function in bufmgr package.
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage) throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     */
    private void unpinPage(PageId pageno, boolean dirty) throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
        }

    } // end of unpinPage

}