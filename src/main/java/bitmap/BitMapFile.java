package bitmap;

import java.io.IOException;
import java.util.Arrays;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.ColumnarFile;
import diskmgr.Page;
import global.*;
import heap.*;

public class BitMapFile implements GlobalConst {

    private PageId headerPageId;
    private String fileName;
    private BitMapHeaderPage bitMapHeaderPage;

    public BitMapHeaderPage getBitMapHeaderPage() {
        return bitMapHeaderPage;
    }

    public void setBitMapHeaderPage(BitMapHeaderPage bitMapHeaderPage) {
        this.bitMapHeaderPage = bitMapHeaderPage;
    }

    /**
     * Access method to data member.
     *
     * @return Return a BitMapHeaderPage object that is the header page of this
     * bitmap file.
     */

    private PageId getFileEntry(String fileName) throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.getFileEntry(fileName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    private Page pinPage(PageId pageId, Page page) throws PinPageException {
        try {

            SystemDefs.JavabaseBM.pinPage(pageId, page, false/* Rdisk */);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }

    private void addFileEntry(String fileName, PageId pageId) throws AddFileEntryException {
        try {
            SystemDefs.JavabaseDB.addFileEntry(fileName, pageId);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AddFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageId) throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageId, false);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    private void freePage(PageId pageId) throws FreePageException {
        try {
            SystemDefs.JavabaseBM.freePage(pageId);
        } catch (Exception e) {
            e.printStackTrace();
            throw new FreePageException(e, "");
        }

    }

    private void deleteFileEntry(String fileName) throws DeleteFileEntryException {
        try {
            SystemDefs.JavabaseDB.deleteFileEntry(fileName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DeleteFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageId, boolean dirty) throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageId, dirty);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    public BitMapFile(String fileName)
            throws GetFileEntryException, PinPageException, ConstructPageException, UnpinPageException {
        headerPageId = getFileEntry(fileName);
        this.fileName = fileName;
        bitMapHeaderPage = new BitMapHeaderPage(true);
        pinPage(headerPageId, bitMapHeaderPage);
        unpinPage(headerPageId, false);
    }

    public BitMapFile(String fileName, long totalTuples) throws AddFileEntryException, IOException,
            ConstructPageException, GetFileEntryException, PinPageException, UnpinPageException {

        headerPageId = getFileEntry(fileName);
        if (headerPageId == null) // file not exist
        {
            bitMapHeaderPage = new BitMapHeaderPage(false);
            headerPageId = bitMapHeaderPage.getCurrPage();
            addFileEntry(fileName, headerPageId);
            init(totalTuples);
            unpinPage(headerPageId, true);
        }

    }

    private void init(long totalTuples) throws PinPageException, UnpinPageException, IOException {
        BMPage bmPage = new BMPage();
        PageId pageId = new PageId();
        int numPages = 1 + (int) totalTuples / (bmPage.getAvailableMap());
        allocatePage(pageId, numPages);
        pinPage(pageId, bmPage);
        bmPage.init(pageId, bmPage);
        unpinPage(pageId, true);
    }

    public BitMapFile(String fileName, ColumnarFile columnarFile, int columnNo, ValueClass value)
            throws GetFileEntryException, ConstructPageException, IOException, AddFileEntryException,
            UnpinPageException, InvalidTupleSizeException, PinPageException, HFBufMgrException,
            InvalidSlotNumberException, BitMapFileCreationException {

        headerPageId = getFileEntry(fileName);
        if (headerPageId == null) // file not exist
        {
            bitMapHeaderPage = new BitMapHeaderPage(false);
            headerPageId = bitMapHeaderPage.getCurrPage();
            addFileEntry(fileName, headerPageId);
            bitMapHeaderPage.setColumnIndex((short) columnNo);
            bitMapHeaderPage.setValueType((short) value.getValueType());

        } else {
            throw new BitMapFileCreationException(null, "File already present");
        }
        init(value, columnarFile, columnNo);
        this.fileName = fileName;
        unpinPage(headerPageId, true);
    }

    /*
     * Initialize our bitMap
     */
    private boolean compareValues(int a, int b) {
        if (a == b)
            return true;
        else
            return false;
    }

    private boolean compareValues(float a, float b) {
        if (a == b)
            return true;
        else
            return false;
    }

    private boolean compareValues(String a, String b) {
        if (a.equals(b))
            return true;
        else
            return false;
    }

    private boolean compareValues(int c, int a, int b) {
        // a must be smaller than b
        if (c >= a && c <= b)
            return true;
        else
            return false;
    }

    private boolean compareValues(float c, float a, float b) {
        // a must be smaller than b
        if (c >= a && c <= b)
            return true;
        else
            return false;
    }

    private boolean compareValues(String c, String a, String b) {
        // a must be smaller than b
        if (c.compareTo(a) >= 0 && c.compareTo(b) >= 0)
            return true;
        else
            return false;
    }

    private void init(ValueClass value, ColumnarFile columnarFile, int columnNo)
            throws IOException, InvalidTupleSizeException, PinPageException, UnpinPageException, HFBufMgrException,
            InvalidSlotNumberException {
        int valType = value.getValueType();
        boolean flag = false;
        int position = 0;

        switch (valType) {
            case 0:
                setupStringBitMap((String) value.getValue(), columnarFile, columnNo);
                break;
            case 1:
                setupIntBitMap((Integer) value.getValue(), columnarFile, columnNo);
                break;
            case -1:
                break;
        }
    }

    private void setupStringBitMap(String value, ColumnarFile columnarFile, int columnNo)
            throws IOException, InvalidTupleSizeException, PinPageException, UnpinPageException, HFBufMgrException,
            InvalidSlotNumberException {
        Scan scan = new Scan(columnarFile, (short) columnNo);
        pinPage(headerPageId, bitMapHeaderPage);
        RID rid = scan.getFirstRID();
        Tuple tuple = scan.getNext(rid);
        BMPage bmPage = new BMPage();

        PageId pageId = new PageId();
        int position = 0;
        allocatePage(pageId, 1);
        bitMapHeaderPage.setNextPage(pageId);
        unpinPage(headerPageId, true);
        pinPage(pageId, bmPage);
        bmPage.init(pageId, bmPage);
        while (tuple != null) {
            String val = Convert.getStringValue(0, tuple.getTupleByteArray(),
                    columnarFile.getColumnarHeader().getColumns()[columnNo].getSize());
            if (val.equals(value)) {
                bmPage.setABit(position, 1);
            }
            int tempAns = bmPage.getAvailableSpace() - 1;
            bmPage.setAvailableSpace(tempAns);

            if (tempAns == 0) {
                PageId pageIdTemp = new PageId();
                allocatePage(pageIdTemp, 1);
                bmPage.setNextPage(pageIdTemp);
                unpinPage(pageId, true);
                pageId = pageIdTemp;
                pinPage(pageId, bmPage);
                bmPage.init(pageId, bmPage);
            }

            tuple = scan.getNext(rid);
            position++;
        }
        bmPage.setNextPage(new PageId(-1));
        unpinPage(pageId, true);
        scan.closeScan();
    }

    private void setupIntBitMap(Integer value, ColumnarFile columnarFile, int columnNo)
            throws IOException, InvalidTupleSizeException, PinPageException, UnpinPageException {
        Scan scan = new Scan(columnarFile, (short) columnNo);
        RID rid = scan.getFirstRID();
        BMPage bitmap = new BMPage();
        Tuple tuple = scan.getNext(rid);
        BMPage bmPage = new BMPage();

        PageId pageId = new PageId();
        int position = 0;
        allocatePage(pageId, 1);
        pinPage(pageId, bmPage);
        bmPage.init(pageId, bmPage);

        while (tuple != null) {
            int val = Convert.getIntValue(0, tuple.getTupleByteArray());
            if (val == value) {
                bmPage.setABit(position, 1);
            }
            int tempAns = bmPage.getAvailableSpace() - 1;
            bmPage.setAvailableSpace(tempAns);

            if (tempAns == 0) {
                PageId pageIdTemp = new PageId();
                allocatePage(pageIdTemp, 1);
                bmPage.setNextPage(pageIdTemp);
                unpinPage(pageId, true);
                pageId = pageIdTemp;
                pinPage(pageId, bmPage);
                bmPage.init(pageId, bmPage);
            }

            tuple = scan.getNext(rid);
            position++;
        }
        unpinPage(pageId, true);
        scan.closeScan();
    }

    public void allocatePage(PageId pageId, int runSize) {
        try {
            SystemDefs.JavabaseDB.allocatePage(pageId, runSize);
        } catch (Exception e) {

        }
    }

    public void close()
            throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        /**********************
         * Why do we need this??
         *********************/
        if (bitMapHeaderPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
            bitMapHeaderPage = null;
        }
    }

    public void destroyBitMapFile() {

    }

    public boolean setBMPagePositions(int position, int bit) throws PinPageException, IOException, UnpinPageException {
        pinPage(headerPageId, bitMapHeaderPage);
        PageId pageId = bitMapHeaderPage.getNextPage();
        BMPage bmPage = new BMPage();
        pinPage(pageId, bmPage);
        int bytes = (position + 8) / 8;
        int locationUntilLoop = bytes / bmPage.getAvailableMap();

        for (int i = 0; i < locationUntilLoop; i++) {
            PageId nextPageId = bmPage.getNextPage();
            bmPage.setCount((short) bmPage.getAvailableMap());
            if (nextPageId.pid == INVALID_PAGE) {
                allocatePage(nextPageId, 1);
                bmPage.setNextPage(nextPageId);
            }
            unpinPage(pageId);
            pageId = nextPageId;
            pinPage(pageId, bmPage);
        }
        unpinPage(pageId, true);
        bmPage.setABit(position, bit);
        unpinPage(headerPageId, false);
        return false;
    }

    public boolean Delete(int position) throws PinPageException, IOException, UnpinPageException {
        return setBMPagePositions(position, 0);
    }

    public boolean Insert(int position) throws PinPageException, IOException, UnpinPageException {
        return setBMPagePositions(position, 1);
    }

    public PageId getHeaderPageId() {
        return headerPageId;
    }

}