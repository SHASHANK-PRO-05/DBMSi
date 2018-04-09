package iterator;

import java.io.IOException;

import bitmap.AddFileEntryException;
import bitmap.ConstructPageException;
import bitmap.GetFileEntryException;
import bitmap.PinPageException;
import bitmap.UnpinPageException;
import columnar.ColumnarFile;
import columnar.ColumnarFileDoesExistsException;
import columnar.ColumnarFilePinPageException;
import columnar.ColumnarFileUnpinPageException;
import columnar.TupleScan;
import diskmgr.DiskMgrException;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import global.TID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.HFPage;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.Tuple;

public class ColumnarSort implements GlobalConst{
	private Heapfile sortedHeapFileNames[];
	
	//Algo
	//create a new heap file.
	//loop around the pages of old heap file.
	//apply in-memory sort for every page
	//write the page into the new heap file.
	//I will now have the number of heap files.
	//run a loop dividing by 2 
	//run untill divide by 2 results into 1 file.
	
	
	
	
	public ColumnarSort(String columnarFileName, int ColumnNo) 
			throws DiskMgrException, 
			ColumnarFileDoesExistsException, 
			ColumnarFilePinPageException, 
			HFException, 
			HFBufMgrException, 
			HFDiskMgrException, 
			ColumnarFileUnpinPageException, 
			PinPageException, 
			AddFileEntryException, 
			UnpinPageException, 
			ConstructPageException, 
			GetFileEntryException, 
			IOException, 
			InvalidTupleSizeException {
		ColumnarFile columnarFile = new ColumnarFile(columnarFileName);
		int numOfColumn = columnarFile.getNumColumns();
		sortedHeapFileNames = new Heapfile[numOfColumn];
		//create the heap files for the sorted  records
		for (int i = 0; i < numOfColumn; i++) {
            String fileNum = Integer.toString(i);
            String columnsFileName = columnarFileName + "." + fileNum + ".sort";
            sortedHeapFileNames[i] = new Heapfile(columnsFileName); 
        }	
		//now insert the sorted pages in the heap files.
		//individual pages would be sorted, not the whole file
		init(columnarFile);
		
		
	}

	private void init(ColumnarFile columnarFile) 
			throws InvalidTupleSizeException, 
			IOException, 
			HFBufMgrException {
	
		Heapfile[] heapfiles = columnarFile.getHeapFileNames();
		
		for(int i = 0;i< heapfiles.length;i++) {
			PageId currentDirPageId = new PageId(heapfiles[i].get_firstDirPageId().pid);
			HFPage currentDirPage = new HFPage();
			pinPage(currentDirPageId, currentDirPage, false/*read disk*/);
			Tuple atuple = new Tuple();
			RID currentDataPageRid = new RID();
			while (currentDirPageId.pid != INVALID_PAGE) {
				for (currentDataPageRid = currentDirPage.firstRecord();
		                 currentDataPageRid != null;
		                 currentDataPageRid = currentDirPage.nextRecord(currentDataPageRid)) {
					
					//write the sorting code
					
					
					
				}
				
				
				
				
				
				
				
			}
			
			
			
			
			
			
			
		}
		
		
		
		
		
		
		
		
		
	}
	
	
	/**
     * short cut to access the pinPage function in bufmgr package.
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
        }

    } // end of unpinPage

	

}
