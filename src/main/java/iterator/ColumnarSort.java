package iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import bitmap.AddFileEntryException;
import bitmap.ConstructPageException;
import bitmap.GetFileEntryException;
import bitmap.PinPageException;
import bitmap.UnpinPageException;
import columnar.ColumnarFile;
import columnar.ColumnarFileDoesExistsException;
import columnar.ColumnarFilePinPageException;
import columnar.ColumnarFileUnpinPageException;
import diskmgr.DiskMgrException;
import diskmgr.Page;
import global.AttrType;
import global.Convert;
import global.GlobalConst;
import global.IntegerValue;
import global.PageId;
import global.RID;
import global.StringValue;
import global.SystemDefs;
import global.ValueClass;
import heap.DataPageInfo;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.HFPage;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;

public class ColumnarSort implements GlobalConst {
	private Heapfile sortedHeapFileNames[];
	private Heapfile heapfiles[];
	private int coloumnNo;
	private int numOfColumn;
	// Algo
	// create a new heap file.
	// loop around the pages of old heap file.
	// apply in-memory sort for every page
	// write the page into the new heap file.
	// I will now have the number of heap files.
	// run a loop dividing by 2
	// run untill divide by 2 results into 1 file.

	public ColumnarSort(String columnarFileName, int columnNo) throws DiskMgrException, ColumnarFileDoesExistsException,
			ColumnarFilePinPageException, HFException, HFBufMgrException, HFDiskMgrException,
			ColumnarFileUnpinPageException, PinPageException, AddFileEntryException, UnpinPageException,
			ConstructPageException, GetFileEntryException, IOException, InvalidTupleSizeException, Exception {
		ColumnarFile columnarFile = new ColumnarFile(columnarFileName);
		this.coloumnNo = columnNo;

		numOfColumn = columnarFile.getNumColumns();
		sortedHeapFileNames = new Heapfile[numOfColumn];
		// create the heap files for the sorted records
		String columnsFileName = "";
		columnsFileName = columnarFileName + "." + columnNo + ".sort";
		// sortedHeapFileNames = new Heapfile(columnsFileName);
		for (int i = 0; i < numOfColumn; i++) {
			String fileNum = Integer.toString(i);

			sortedHeapFileNames[i] = new Heapfile(columnsFileName);

		}
		// now insert the sorted pages in the heap files.
		// individual pages would be sorted, not the whole file
		int numDataPages = init(columnarFile);
		// Real work begins
		mergesort(numDataPages);

	}

	private void mergesort(int numDataPages) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
			InvalidSlotNumberException, InvalidTupleSizeException, Exception {
		// TODO Auto-generated method stub
		Heapfile sortHeapFile = sortedHeapFileNames[coloumnNo];
		PageId currentDirPageId;
		HFPage currentDirPage;
		RID currentDataPageRid = new RID();
		Tuple atuple = new Tuple();
		HFPage currentDataPage1 = new HFPage();
		HFPage currentDataPage2 = new HFPage();
		int mergeloopNum = (int) Math.ceil((Math.log(numDataPages) / Math.log(2)));

		for (int i = 0; i < mergeloopNum; i++) {
			Heapfile sort1 = new Heapfile("1");

			/*
			 * here is the code to get the first data page
			 * 
			 */
			currentDirPageId = new PageId(sortHeapFile.get_firstDirPageId().pid);
			currentDirPage = new HFPage();
			pinPage(currentDirPageId, currentDirPage, false/* read disk */);
			currentDataPageRid = currentDirPage.firstRecord();
			try {
				atuple = currentDirPage.getRecord(currentDataPageRid);
			} catch (InvalidSlotNumberException e)// check error! return false(done)
			{
				return;
			}
			DataPageInfo dpinfo = new DataPageInfo(atuple);
			try {
				// pin the datapage
				pinPage(dpinfo.pageId, currentDataPage1, false/* Rddisk */);
				// check error;need unpin currentDirPage
			} catch (Exception e) {
				unpinPage(currentDirPageId, false/* undirty */);
				throw e;
			}
			/*
			 * above is the code to get the first data page
			 * 
			 */
			currentDataPage2.setCurPage(currentDataPage1.getCurPage());
			for (int j = 0; j < Math.pow(2, i); j++) {
				currentDataPage2.setCurPage(currentDataPage2.getNextPage());

			}

			mergeRuns(currentDataPage1.getCurPage(), currentDataPage2.getCurPage(), 1);

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
		Heapfile heapfile = new Heapfile("Sort");
		int count = 0;
		HFPage list1 = new HFPage();
		HFPage list2 = new HFPage();
		PageId nextPage1 = new PageId();
		PageId nextPage2 = new PageId();
		RID record1 = new RID();
		RID record2 = new RID();
		// it is for int later will change
		ValueClass val1;
		ValueClass val2;
		int counter1 = 0;
		int counter2 = 0;
		pinPage(curPage, list1, false);
		pinPage(curPage2, list2, false);
		record1 = list1.firstRecord();
		record2 = list2.firstRecord();
		while (record1 != null && record2 != null) {
			val1 = new IntegerValue(Convert.getIntValue(0, list1.getRecord(record1).getTupleByteArray()));
			val2 = new IntegerValue(Convert.getIntValue(0, list2.getRecord(record1).getTupleByteArray()));

			if ((Integer) val1.getValue() < (Integer) val2.getValue()) {
				heapfile.insertRecord(list1.getRecord(record1).getTupleByteArray());
				record1 = list1.nextRecord(record1);
				if (record1 == null && counter1 < i) {
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
					}
					record1 = list1.firstRecord();

				}
			}

			else if ((Integer) val1.getValue() > (Integer) val2.getValue()) {
				heapfile.insertRecord(list1.getRecord(record2).getTupleByteArray());
				record2 = list2.nextRecord(record2);
				if (record2 == null && counter2 < i) {
					nextPage2 = list1.getNextPage();
					try {
						unpinPage(curPage2, false /* undirty */);
					} catch (Exception e) {
						throw new HFException(e, "heapfile,_find unpinpage failed");
					}

					curPage2.pid = nextPage2.pid;
					if (curPage.pid != INVALID_PAGE) {
						counter2++;
						pinPage(curPage2, list2, false);
					}
					record2 = list2.firstRecord();

				}

			} else {
				heapfile.insertRecord(list1.getRecord(record2).getTupleByteArray());
				heapfile.insertRecord(list1.getRecord(record2).getTupleByteArray());
				record1 = list1.nextRecord(record1);
				record2 = list2.nextRecord(record2);
				if (record1 == null && counter1 < i) {
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
						
					}
					record1 = list1.firstRecord();

				}
				if (record2 == null && counter2 < i) {
					nextPage2 = list1.getNextPage();
					try {
						unpinPage(curPage2, false /* undirty */);
					} catch (Exception e) {
						throw new HFException(e, "heapfile,_find unpinpage failed");
					}

					curPage2.pid = nextPage2.pid;
					if (curPage.pid != INVALID_PAGE) {
						counter2++;
						pinPage(curPage2, list2, false);
					}
					record2 = list2.firstRecord();

				}
				

			}

		}
		while (record1 == null && record2 != null) {
			heapfile.insertRecord(list2.getRecord(record2).getTupleByteArray());
			record2 = list2.nextRecord(record2);
			if (record2 == null && counter2 < i) {
				nextPage2 = list1.getNextPage();
				try {
					unpinPage(curPage2, false /* undirty */);
				} catch (Exception e) {
					throw new HFException(e, "heapfile,_find unpinpage failed");
				}

				curPage2.pid = nextPage2.pid;
				if (curPage.pid != INVALID_PAGE) {
					counter2++;
					pinPage(curPage2, list2, false);
				}
				record2 = list2.firstRecord();
			}
		}
		while (record1 != null && record2 == null) {
			heapfile.insertRecord(list1.getRecord(record1).getTupleByteArray());
			record1 = list1.nextRecord(record2);
			if (record1 == null && counter1 < i) {
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
				}
				record1 = list1.firstRecord();
			}
			
		}

	}
	/*
	 * 
	 * function to get the first list of sorted page
	 */

	private int init(ColumnarFile columnarFile)
			throws InvalidTupleSizeException, IOException, HFBufMgrException, Exception {

		heapfiles = columnarFile.getHeapFileNames();
		// can throw error if columnNo is greater than the heapfiles size
		AttrType sortedColumnType = columnarFile.getColumnInfo(coloumnNo);
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
		int dirCount = 0;
		int datapageCount = 0;
		int totalPageCount = 0;
		int size = sortedColumnType.getSize();

		// loop around the directory pages
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
				count = 0;
				// loop around the records
				for (record = currentDataPage.firstRecord(); record != null; record = currentDataPage
						.nextRecord(record)) {
					if (sortedColumnType.getAttrType() == 0) {
						sortingList.add(new SortInfo(new StringValue(
								Convert.getStringValue(0, currentDataPage.getRecord(record).getTupleByteArray(), size)),
								count));
					} else if (sortedColumnType.getAttrType() == 1) {
						sortingList.add(new SortInfo(
								new IntegerValue(
										Convert.getIntValue(0, currentDataPage.getRecord(record).getTupleByteArray())),
								count));
					}
					count++;

				}

				// unpin page immediately
				unpinPage(dpinfo.pageId, false);

				// sort a page using comparator
				sortingList.sort(new Comparator<SortInfo>() {
					public int compare(SortInfo o1, SortInfo o2) {
						// TODO Auto-generated method stub
						return o1.compareTo(o2);
					}
				});

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
					sortedHeapFileNames[coloumnNo].insertRecord(recPtr);

				}
				sortOtherHeapFilesInit(dirCount, datapageCount, size, sortingList);
				datapageCount++;
			}
			// tracing
			for (SortInfo val : sortingList) {
				System.out.println(val.getVal().getValue());
			}
			nextDirPageId = currentDirPage.getNextPage();
			try {
				unpinPage(currentDirPageId, false /* undirty */);
			} catch (Exception e) {
				throw new HFException(e, "heapfile,_find,unpinpage failed");
			}

			currentDirPageId.pid = nextDirPageId.pid;
			if (currentDirPageId.pid != INVALID_PAGE) {
				dirCount++;
				datapageCount = 0;
				pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);
				if (currentDirPage == null)
					throw new HFException(null, "pinPage return null page");
			}

		}
		return totalPageCount;
	}

	/*
	 * Sort other heap file according to the sorted column for first run
	 * 
	 */
	private void sortOtherHeapFilesInit(int dirCount, int datapageCount, int size, ArrayList<SortInfo> sortingList)
			throws HFBufMgrException, IOException, InvalidTupleSizeException, HFException, Exception {
		// loop around heap files
		Heapfile sortedheapfile;
		PageId currentDirPageId;
		HFPage currentDirPage;
		HFPage currentDataPage;
		PageId nextDirPageId;

		Tuple atuple;
		RID currentDataPageRid;
		int matchdirCount;
		int matchdataCount;
		for (int i = 0; i < numOfColumn; i++) {
			if (i == coloumnNo)
				continue;
			sortedheapfile = sortedHeapFileNames[i];
			currentDirPageId = new PageId(heapfiles[i].get_firstDirPageId().pid);
			currentDirPage = new HFPage();
			currentDataPage = new HFPage();
			nextDirPageId = new PageId();
			atuple = new Tuple();
			currentDataPageRid = new RID();
			matchdirCount = 0;
			matchdataCount = 0;
			pinPage(currentDirPageId, currentDirPage, false/* read disk */);
			// loop around dirctory
			while (currentDirPageId.pid != INVALID_PAGE && matchdirCount < dirCount) {
				nextDirPageId = currentDirPage.getNextPage();
				try {
					unpinPage(currentDirPageId, false /* undirty */);
				} catch (Exception e) {
					throw new HFException(e, "heapfile,_find,unpinpage failed");
				}

				currentDirPageId.pid = nextDirPageId.pid;
				if (currentDirPageId.pid != INVALID_PAGE) {
					matchdirCount++;
					pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);
					if (currentDirPage == null)
						throw new HFException(null, "pinPage return null page");
				}

			}

			if (currentDirPageId.pid != INVALID_PAGE && matchdirCount == dirCount) {

				// loop around the directory page
				for (currentDataPageRid = currentDirPage.firstRecord(); currentDataPageRid != null
						&& matchdataCount < datapageCount; currentDataPageRid = currentDirPage
								.nextRecord(currentDataPageRid)) {
				}
			}
			// no work of directory now!unpin it
			if (currentDirPageId.pid != INVALID_PAGE) {
				unpinPage(currentDirPageId, false /* undirty */);
			}

			// now we have directory and dataPage
			try {
				atuple = currentDirPage.getRecord(currentDataPageRid);
			} catch (InvalidSlotNumberException e)// check error! return false(done)
			{
				return;
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

			// traverse the list and take out the slots.
			byte[] dataByte = new byte[size];
			for (SortInfo sortInfo : sortingList) {
				int pos = sortInfo.position;
				RID rid = new RID(currentDataPage.getCurPage(), pos);
				dataByte = currentDataPage.getDataAtSlot(rid);
				sortedheapfile.insertRecord(dataByte);

			}
			// unpin the datapage
			unpinPage(dpinfo.pageId, false);
		}
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
