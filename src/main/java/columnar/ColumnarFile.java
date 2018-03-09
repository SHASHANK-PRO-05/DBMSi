package columnar;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import bufmgr.BufMgr;
import diskmgr.DiskMgrException;
import diskmgr.FileNameTooLongException;
import diskmgr.Page;
import global.*;
import heap.*;
import heap.InvalidTupleSizeException;

public class ColumnarFile implements GlobalConst {

    private ColumnarHeader columnarHeader;
    //Shashank: I am not sure if it is required
    private Heapfile heapFileNames[];
    private int numColumns;
    /*
     * Contructor for initialization
     * @param filename: dbname
     * @param numColumns; number of columns
     * @param type : attribute information
     */

    public ColumnarFile(String fileName, int numColumns, AttrType[] type)
            throws ColumnClassCreationException, HFDiskMgrException,
            IOException {

        try {
            columnarHeader = new ColumnarHeader(fileName, numColumns, type);
            heapFileNames = new Heapfile[numColumns];
            for (int i = 0; i < numColumns; i++) {
                String fileNum = Integer.toString(i);
                String columnsFileName = fileName + "." + fileNum;
                heapFileNames[i] = new Heapfile(columnsFileName);
            }

        } catch (Exception e) {
            e.printStackTrace();
            for (int i = 0; i < numColumns; i++) {
                String fileNum = Integer.toString(i);
                String columnsFileName = fileName + "." + fileNum;
                deleteFileEntry(columnsFileName);
            }
            PageId pageId = columnarHeader.getCurPage();
            PageId nextPage = columnarHeader.getNextPage();

            while (pageId.pid != INVALID_PAGE) {
                deallocatePage(pageId);
                pageId.pid = nextPage.pid;
            }
            throw new ColumnClassCreationException(e
                    , "ColumnarFile: not able to create a file");
        }
    }
    
    /*
     * constructor for opening the db
     */
    public ColumnarFile(String fileName)
            throws IOException, DiskMgrException
            , ColumnarFileDoesExistsException
            , ColumnarFilePinPageException {
        PageId pageId = getFileEntry(fileName);
        if (pageId != null) {
            columnarHeader = new ColumnarHeader(pageId, fileName);
        } else {
            throw new ColumnarFileDoesExistsException(null
                    , "Columnar File Does not exists");
        }
    }
    /*
     * Deletes whole Database
     * Not completed yet
     */
    public void deleteColumnarFile()
            throws InvalidSlotNumberException,
            FileAlreadyDeletedException,
            InvalidTupleSizeException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException,
            ColumnarFilePinPageException,
            ColumnarFileUnpinPageException,
            HFException {
        String fname = this.getColumnarHeader().getHdrFile();
        PageId pageId = this.getColumnarHeader().getHeaderPageId();
        HFPage hfPage = new HFPage();
        pinPage(pageId, hfPage);
        for (int i = 0; i < numColumns; i++) {
            Heapfile hf = new Heapfile(fname + "i");
            hf.deleteFile();
        }
        unpinPage(pageId, false);
        deleteFileEntry(columnarHeader.getHdrFile());

    }

    /*
     * insert a tuple in the heapfile
     * @param bytePtr: saves the information of tuple 
     * return: TID 
     */
    public TID insertTuple(byte[] bytePtr) throws Exception {

        ByteToTuple byteToTuple
                = new ByteToTuple(this.getColumnarHeader().getColumns());
        ArrayList<byte[]> arrayList = byteToTuple.setTupleBytes(bytePtr);
        RID[] rids = new RID[arrayList.size()];
        for (int i = 0; i < arrayList.size(); i++) {
            Heapfile heapfile = new Heapfile(this.getColumnarHeader().getHdrFile() + "." + i);
            rids[i] = heapfile.insertRecord(arrayList.get(i));
           // System.out.println("RID"+rids[i]);
        }

        return new TID(rids.length, this.getTupleCount(), rids);
    }
    /*
     * gives the count of tuple 
     * return: Integer - count of total records
     */
    public int getTupleCount() throws Exception {
        String fileName = this.getColumnarHeader().getHdrFile() + ".0";
        Heapfile heapfile = new Heapfile(fileName);
        return heapfile.getRecCnt();
    }
    
    public Tuple getTuple(TID tid) 
    		throws InvalidSlotNumberException, 
    		InvalidTupleSizeException, 
    		Exception {
		StringBuffer stringBuffer = new StringBuffer();
    	for(int i=0;i<tid.getNumRIDs();i++) {
    		Heapfile heapFile = new Heapfile(this.getColumnarHeader().getHdrFile() + "." + i);
    		Tuple tuple = heapFile.getRecord(tid.getRecordIDs()[i]);
    		int length=tuple.getLength()-tuple.getOffset();
    		byte[] by = new byte[length]; 
    		System.arraycopy(tuple.returnTupleByteArray(), tuple.getOffset(), by, 0,length );
    		
    		
    		stringBuffer.append(by.toString());
    	}
    	
    	return new Tuple(stringBuffer.toString().getBytes(),0,stringBuffer.length());
    	
    }
    
    boolean updateTuple(TID tid, Tuple newTuple ) 
    		throws InvalidSlotNumberException, 
    		InvalidTupleSizeException, 
    		Exception {
    	
    	int length=newTuple.getLength()-newTuple.getOffset();
    	byte[] newTupleBytes = new byte[length];
    	ByteToTuple byteToTuple = new ByteToTuple(this.getColumnarHeader().getColumns());
    	System.arraycopy(newTuple.returnTupleByteArray(), newTuple.getOffset(), newTupleBytes, 0,length );
    	ArrayList<byte[]> arrayList = byteToTuple.setTupleBytes(newTupleBytes);
    	for(int i=0;i<tid.getNumRIDs();i++) {
    		Tuple temp = new Tuple(arrayList.get(i),0,arrayList.get(i).length);
    		Heapfile heapFile = new Heapfile(this.getColumnarHeader().getHdrFile() + "." + i);
    		boolean result = heapFile.updateRecord(tid.getRecordIDs()[i], temp);
    		if(result==false)
    			return false;
    	}
    	
    	return true;
    }
    
    boolean updateColumnOfTuple(TID tid, Tuple newTuple, int column) 
    		throws InvalidSlotNumberException, 
    		InvalidUpdateException, 
    		InvalidTupleSizeException, 
    		Exception
    {
    	Heapfile heapFile = new Heapfile(this.getColumnarHeader().getHdrFile() + "." + column);
    	int length = newTuple.getLength()-newTuple.getOffset();
    	byte[] newTupleBytes = new byte[length];
    	ByteToTuple byteToTuple = new ByteToTuple(this.getColumnarHeader().getColumns());
    	System.arraycopy(newTuple.returnTupleByteArray(), newTuple.getOffset(), newTupleBytes, 0,length );
    	ArrayList<byte[]> arrayList = byteToTuple.setTupleBytes(newTupleBytes);
    	Tuple temp = new Tuple(arrayList.get(column),0,arrayList.get(column).length);
    	boolean result = heapFile.updateRecord(tid.getRecordIDs()[column], temp);
		if(result==false)
			return false;
		else
			return true;   	
    	
    }
    
    /*
     * setup functions 
     * starts here
     */
    private void pinPage(PageId pageId, Page page) throws
            ColumnarFilePinPageException {
        try {
            SystemDefs.JavabaseBM.pinPage(pageId, page, false);
        } catch (Exception e) {
            throw new ColumnarFilePinPageException(e,
                    "Columnar: Not able to pin page");
        }
    }

    private void unpinPage(PageId pageId, boolean dirty) throws ColumnarFileUnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageId, dirty);
        } catch (Exception e) {
            throw new ColumnarFileUnpinPageException(e, "Columnar: not able to unpin");
        }
    }

    private PageId getFileEntry(String fileName) throws IOException, DiskMgrException {
        return SystemDefs.JavabaseDB.getFileEntry(fileName);
    }

    private void deallocatePage(PageId pageId) throws HFDiskMgrException {
        try {
            SystemDefs.JavabaseDB.deallocatePage(pageId, 1);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: deallocatePage failed");
        }
    }
    /*
     * setup file ends here
     */
    
    
    /*
     * deletes a single file entry
     */
    private void deleteFileEntry(String filename)
            throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.deleteFileEntry(filename);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: deleteFileEntry() failed");
        }

    }
    /*
     * getter-setters starts here
     */
    public ColumnarHeader getColumnarHeader() {
        return columnarHeader;
    }

    public void setColumnarHeader(ColumnarHeader columnarHeader) {
        this.columnarHeader = columnarHeader;
    }

    public Heapfile[] getHeapFileNames() {
        return heapFileNames;
    }

    public void setHeapFileNames(Heapfile[] heapFileNames) {
        this.heapFileNames = heapFileNames;
    }

    public int getNumColumns() {
        return numColumns;
    }

    public void setNumColumns(int numColumns) {
        this.numColumns = numColumns;
    }
    
    
    /*
     * getter-setter ends here
     */
}