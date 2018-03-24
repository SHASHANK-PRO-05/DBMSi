package columnar;

import bitmap.BitMapFile;
import btree.*;
import diskmgr.DiskMgrException;
import diskmgr.Page;
import global.*;
import heap.*;
import heap.InvalidTupleSizeException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ColumnarFile implements GlobalConst {

    private ColumnarHeader columnarHeader;

    private Heapfile heapFileNames[];
    private int numColumns;

    /*
     * Contructor for initialization
     *
     * @param filename: dbname
     *
     * @param numColumns; number of columns
     *
     * @param type : attribute information
     */

    public ColumnarFile(String fileName, int numColumns, AttrType[] type)
            throws ColumnClassCreationException, HFDiskMgrException, IOException {

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
            throw new ColumnClassCreationException(e, "ColumnarFile: not able to create a file");
        }
    }

    /*
     * constructor for opening the db
     */

    public ColumnarFile(String fileName)
            throws IOException, DiskMgrException, ColumnarFileDoesExistsException, ColumnarFilePinPageException,
            HFException, HFBufMgrException, HFDiskMgrException, ColumnarFileUnpinPageException {
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
            throw new ColumnarFileDoesExistsException(null, "Columnar File Does not exists");
        }
    }

    // TODO: change the throwing exceptions
    public boolean createBitMapIndex(int columnNo, ValueClass valueClass) throws Exception {
        String fileName = this.getColumnarHeader().getHdrFile() + "." + columnNo + "." + valueClass.getValue();
        BitMapFile bitMapFile = new BitMapFile(fileName, this, columnNo, valueClass);
        return true;
    }

    /*
     * Deletes whole Database Not completed yet
     */
    public void deleteColumnarFile() throws InvalidSlotNumberException, FileAlreadyDeletedException,
            InvalidTupleSizeException, HFBufMgrException, HFDiskMgrException, IOException, ColumnarFilePinPageException,
            ColumnarFileUnpinPageException, HFException, heap.InvalidSlotNumberException {
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
     *
     * @param bytePtr: saves the information of tuple return: TID
     */
    public TID insertTuple(byte[] bytePtr) throws Exception {

        DirectoryHFPage directoryHFPage = new DirectoryHFPage();
        pinPage(this.getColumnarHeader().getHeaderPageId(), directoryHFPage);
        ByteToTuple byteToTuple = new ByteToTuple(this.getColumnarHeader().getColumns());
        ArrayList<byte[]> arrayList = byteToTuple.setTupleBytes(bytePtr);
        RID[] rids = new RID[arrayList.size()];
        for (int i = 0; i < arrayList.size(); i++) {
            Heapfile heapfile = new Heapfile(this.getColumnarHeader().getHdrFile() + "." + i);
            rids[i] = heapfile.insertRecord(arrayList.get(i));
        }

        long pos = directoryHFPage.getReccnt() + 1;
        directoryHFPage.setReccnt(pos);
        TID tid = new TID(rids.length, (int) pos, rids);
        updateIndex(tid, arrayList, (int) pos);
        unpinPage(this.getColumnarHeader().getHeaderPageId(), true);
        return tid;
    }


    private void updateIndex(TID tid, ArrayList<byte[]> arrayList, int position)
            throws ColumnarFilePinPageException,
            ColumnarFileUnpinPageException,
            IOException
            , HFBufMgrException, InvalidSlotNumberException
            , ConstructPageException, GetFileEntryException
            , btree.PinPageException, Exception {
        AttrType[] attrTypes = this.getColumnarHeader().getColumns();
        ArrayList<IndexInfo> indexInfos = this.getColumnarHeader().getAllIndexes();
        for (int i = 0; i < indexInfos.size(); i++) {
            int columnNumber = indexInfos.get(i).getColumnNumber();
            AttrType attrType = attrTypes[columnNumber];

            ValueClass valueClass;
            KeyClass keyClass;
            if (attrType.getAttrType() == AttrType.attrString) {
                String temp = Convert.getStringValue(0
                        , arrayList.get(columnNumber), attrType.getSize());
                valueClass = new StringValue(temp);
                keyClass = new StringKey(temp);
            } else {
                int temp = Convert
                        .getIntValue(0, arrayList.get(columnNumber));
                valueClass = new IntegerValue(temp);
                keyClass = new IntegerKey(temp);
            }
            //System.out.println(indexInfos.get(i).getIndextype().indexType);
            switch (indexInfos.get(i).getIndextype().indexType) {
                case IndexType.B_Index:
                    BTreeFile bTreeFile = new BTreeFile(indexInfos.get(i).getFileName());
                    bTreeFile.insert(keyClass, tid);
                    bTreeFile.close();
                    break;
                case IndexType.BitMapIndex:
                    BitMapFile bitMapFile = new BitMapFile(indexInfos.get(i).getFileName());
                    if (indexInfos.get(i).getValue().getValue().toString().equals(valueClass.toString())) {
                        bitMapFile.Insert(position);
                    } else {
                        bitMapFile.Delete(position);
                    }
                    bitMapFile.close();
                    break;
            }
        }
    }

    /*
     * gives the count of tuple return: Integer - count of total records
     */
    public long getTupleCount() throws Exception {
        DirectoryHFPage directoryHFPage = new DirectoryHFPage();
        pinPage(this.getColumnarHeader().getHeaderPageId(), directoryHFPage);
        long ans = directoryHFPage.getReccnt();
        unpinPage(this.getColumnarHeader().getHeaderPageId(), false);
        return ans;
    }

    public Tuple getTuple(TID tid) throws InvalidSlotNumberException, InvalidTupleSizeException, Exception {
        StringBuffer stringBuffer = new StringBuffer();
        pinPage(this.getColumnarHeader().getHeaderPageId(), this.getColumnarHeader());
        String fname = this.getColumnarHeader().getHdrFile();
        for (int i = 0; i < tid.getNumRIDs(); i++) {
            Heapfile heapFile = new Heapfile(fname + "." + i);
            Tuple tuple = heapFile.getRecord(tid.getRecordIDs()[i]);
            int length = tuple.getLength() - tuple.getOffset();
            byte[] by = new byte[length];
            System.arraycopy(tuple.returnTupleByteArray(), tuple.getOffset(), by, 0, length);
            stringBuffer.append(by.toString());
        }
        unpinPage(this.getColumnarHeader().getHeaderPageId(), false);

        return new Tuple(stringBuffer.toString().getBytes(), 0, stringBuffer.length());

    }

    boolean updateTuple(TID tid, Tuple newTuple)
            throws InvalidSlotNumberException, InvalidTupleSizeException, Exception {

        int length = newTuple.getLength() - newTuple.getOffset();
        pinPage(this.getColumnarHeader().getHeaderPageId(), this.getColumnarHeader());
        String fname = this.getColumnarHeader().getHdrFile();
        byte[] newTupleBytes = new byte[length];
        ByteToTuple byteToTuple = new ByteToTuple(this.getColumnarHeader().getColumns());
        System.arraycopy(newTuple.returnTupleByteArray(), newTuple.getOffset(), newTupleBytes, 0, length);
        ArrayList<byte[]> arrayList = byteToTuple.setTupleBytes(newTupleBytes);
        for (int i = 0; i < tid.getNumRIDs(); i++) {
            Tuple temp = new Tuple(arrayList.get(i), 0, arrayList.get(i).length);
            Heapfile heapFile = new Heapfile(fname + "." + i);
            boolean result = heapFile.updateRecord(tid.getRecordIDs()[i], temp);
            if (!result)
                return false;
        }
        unpinPage(this.getColumnarHeader().getHeaderPageId(), false);

        return true;
    }

    ValueClass getValue(TID tid, int column) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFDiskMgrException, HFBufMgrException, Exception {

        pinPage(this.getColumnarHeader().getHeaderPageId(), this.getColumnarHeader());
        String fname = this.getColumnarHeader().getHdrFile();
        Heapfile heapFile = new Heapfile(fname + "." + column);
        RID rid = tid.getRecordIDs()[column];
        Tuple tuple = heapFile.getRecord(rid);
        int length = tuple.getLength() - tuple.getOffset();
        byte[] by = new byte[length];
        System.arraycopy(tuple.returnTupleByteArray(), tuple.getOffset(), by, 0, length);
        unpinPage(this.getColumnarHeader().getHeaderPageId(), false);
        if (columnarHeader.getColumns()[column].getAttrType() == 0) {
            StringValue stringValue = new StringValue(by.toString());
            return stringValue;
        } else {
            ByteBuffer bb = ByteBuffer.wrap(by);
            IntegerValue integerValue = new IntegerValue(bb.getInt());
            return integerValue;
        }

    }

    boolean updateColumnOfTuple(TID tid, Tuple newTuple, int column)
            throws InvalidSlotNumberException, InvalidUpdateException, InvalidTupleSizeException, Exception {
        pinPage(this.getColumnarHeader().getHeaderPageId(), this.getColumnarHeader());
        String fname = this.getColumnarHeader().getHdrFile();
        Heapfile heapFile = new Heapfile(fname + "." + column);
        int length = newTuple.getLength() - newTuple.getOffset();
        byte[] newTupleBytes = new byte[length];
        ByteToTuple byteToTuple = new ByteToTuple(this.getColumnarHeader().getColumns());
        System.arraycopy(newTuple.returnTupleByteArray(), newTuple.getOffset(), newTupleBytes, 0, length);
        ArrayList<byte[]> arrayList = byteToTuple.setTupleBytes(newTupleBytes);
        Tuple temp = new Tuple(arrayList.get(column), 0, arrayList.get(column).length);
        boolean result = heapFile.updateRecord(tid.getRecordIDs()[column], temp);
        unpinPage(this.getColumnarHeader().getHeaderPageId(), false);
        if (!result)
            return false;
        else
            return true;
    }

    boolean markTupleDeleted(TID tid) throws Exception {
        String fname = this.getColumnarHeader().getHdrFile() + ".del";
        long totalNumRecords = this.getTupleCount();
        BitMapFile bmFile = new BitMapFile(fname, totalNumRecords);
        if (bmFile.Insert(tid.getPosition())) {
            return true;
        } else
            return false;
    }

    /*
     * setup functions starts here
     */
    private void pinPage(PageId pageId, Page page) throws ColumnarFilePinPageException {
        try {
            SystemDefs.JavabaseBM.pinPage(pageId, page, false);
        } catch (Exception e) {
            throw new ColumnarFilePinPageException(e, "Columnar: Not able to pin page");
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
    private void deleteFileEntry(String filename) throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.deleteFileEntry(filename);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: deleteFileEntry() failed");
        }

    }

    public AttrType getColumnInfo(int i) throws ColumnarFilePinPageException, InvalidSlotNumberException,
            HFBufMgrException, heap.InvalidSlotNumberException, IOException, ColumnarFileUnpinPageException {
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

    public int getNumColumns() throws IOException {
        return this.getColumnarHeader().getColumnCount();
    }

    public void setNumColumns(int numColumns) {
        this.numColumns = numColumns;
    }


    /*
     * getter-setter ends here
     */

}