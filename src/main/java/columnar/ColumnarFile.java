package columnar;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import bitmap.BitMapFile;
import bufmgr.BufMgr;
import diskmgr.DiskMgrException;
import diskmgr.FileNameTooLongException;
import diskmgr.Page;
import global.*;
import heap.*;
import heap.InvalidTupleSizeException;

public class ColumnarFile implements GlobalConst {

    private ColumnarHeader columnarHeader;
    //Question by shashank: I am not sure if it is required
    //But logic is there so we will have to use this.
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
            , ColumnarFilePinPageException
            , HFException, HFBufMgrException,
            HFDiskMgrException,
            ColumnarFileUnpinPageException {
        PageId pageId = getFileEntry(fileName);
        if (pageId != null) {
            columnarHeader = new ColumnarHeader(pageId, fileName);
            pinPage(pageId, columnarHeader);
            heapFileNames = new Heapfile[columnarHeader.getColumnCount()];
            for (int i = 0; i < heapFileNames.length; i++) {
                heapFileNames[i] = new Heapfile(fileName + "." + i);
            }
            unpinPage(pageId, false);
        } else {
            throw new ColumnarFileDoesExistsException(null
                    , "Columnar File Does not exists");
        }
    }

    //TODO: change the throwing exceptions
    public boolean createBitMapIndex(int columnNo, ValueClass valueClass)
            throws Exception {
        String fileName = this.getColumnarHeader().getHdrFile()
                + "." + columnNo + "." + valueClass.getValue();

        BitMapFile bitMapFile = new BitMapFile(fileName
                , this, columnNo, valueClass);

        return true;
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
            HFException, heap.InvalidSlotNumberException {
        String fname = this.getColumnarHeader().getHdrFile();
        PageId pageId = this.getColumnarHeader().getHeaderPageId();
        HFPage hfPage = new HFPage();
        pinPage(pageId, hfPage);
        for (int i = 0; i < numColumns; i++) {
            Heapfile hf = new Heapfile(fname + '.' + i);
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

        ColumnarHeader directoryHFPage = new ColumnarHeader();
        pinPage(this.getColumnarHeader().getHeaderPageId(), directoryHFPage);
        ByteToTuple byteToTuple
                = new ByteToTuple(this.getColumnarHeader().getColumns());
        ArrayList<byte[]> arrayList = byteToTuple.setTupleBytes(bytePtr);
        RID[] rids = new RID[arrayList.size()];
        for (int i = 0; i < arrayList.size(); i++) {
            Heapfile heapfile = new Heapfile(this.getColumnarHeader().getHdrFile() + "." + i);
            //TODO: Exception handling and removal in case
            //TODO: of failures
            rids[i] = heapfile.insertRecord(arrayList.get(i));
        }
        long pos = directoryHFPage.getReccnt() + 1;
        directoryHFPage.setReccnt(pos);
        unpinPage(this.getColumnarHeader().getHeaderPageId(), true);
        return new TID(rids.length, (int) pos, rids);
    }

    /*
     * gives the count of tuple
     * return: Integer - count of total records
     */
    public long getTupleCount() throws Exception {
        DirectoryHFPage directoryHFPage = new DirectoryHFPage();
        pinPage(this.getColumnarHeader().getHeaderPageId(), directoryHFPage);
        long ans = directoryHFPage.getReccnt();
        unpinPage(this.getColumnarHeader().getHeaderPageId(), false);
        return ans;
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

    public AttrType getColumnInfo(int i)
            throws ColumnarFilePinPageException,
            InvalidSlotNumberException,
            HFBufMgrException,
            heap.InvalidSlotNumberException,
            IOException,
            ColumnarFileUnpinPageException {
        DirectoryHFPage dirpage = new DirectoryHFPage();
        PageId id = columnarHeader.getHeaderPageId();
        pinPage(id, dirpage);
        AttrType attrTye = columnarHeader.getColumn(i);
        unpinPage(id, false);
        return attrTye;
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