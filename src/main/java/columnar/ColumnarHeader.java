package columnar;

import java.io.IOException;

import diskmgr.FileNameTooLongException;
import diskmgr.Page;
import global.*;
import heap.*;


public class ColumnarHeader extends HFPage {
    private String hdrFile;
    private PageId headerPageId;
    final private int COLUMNMETA_COLUMNID = 0;
    final private int COLUMNMETA_ATTR = 2;
    final private int COLUMNMETA_SIZE = 4;
    final private int COLUMNMETA_NAME = 6;
    final private int INDEXMATA_COLUMNID = 0;
    final private int INDEXMATA_INDEXTYPE = 2;
    final private int INDEXMATA_FILENAME = 4;
    final private int INDEXMATA_VALUE = 54;
    private int counter;

    public ColumnarHeader(PageId pageId, String tableName) {
        this.headerPageId = pageId;
        this.hdrFile = tableName;
    }

    public ColumnarHeader(String name, int numColumns, AttrType[] type)
            throws HFDiskMgrException,
            HFBufMgrException,
            ColumnHeaderCreateException,
            IOException,
            FileNameTooLongException,
            ColumnarFileExistsException,
            ColumnarNewPageException,
            ColumnarMetaInsertException {
        // checking name entry in DB file
        //On failing as file is not present yet so created HFPage and inserted the name and inserted back
        // to DB file

        if (name.length() >= 50)
            throw new FileNameTooLongException(null, "FILENAME: file name is too long");
        hdrFile = name;
        PageId hdrPageNo = getFileEntry(hdrFile);
        if (hdrPageNo == null) {
            hdrPageNo = newPage(this, 1);
            if (hdrPageNo == null)
                throw new ColumnHeaderCreateException(null, "can't allocate new page");
            addFileEntry(hdrFile, hdrPageNo);
            init(hdrPageNo, this);
            PageId pageId = new PageId(INVALID_PAGE);
            setType((short) numColumns);
            setCounter(0);
            init(type);
            unpinPage(hdrPageNo, true /*dirty*/);
            this.headerPageId = hdrPageNo;
        } else {
            throw new ColumnarFileExistsException(null,
                    "Columnar: Trying to create file existing already");
        }
    }


    public void init(AttrType[] attrTypes) throws IOException
            , HFBufMgrException, ColumnarNewPageException,
            ColumnarMetaInsertException {
        for (int i = 0; i < attrTypes.length; i++) {
            /*
             * attrSize: 2
             * type: 2 bytes
             * name: 50 bytes
             * columnNumber: 2 bytes
             */
            byte[] byteArray = new byte[56];
            Convert.setShortValue((short) attrTypes[i].getColumnId(), COLUMNMETA_COLUMNID, byteArray);
            Convert.setShortValue((short) attrTypes[i].getAttrType(), COLUMNMETA_ATTR, byteArray);
            Convert.setShortValue((short) attrTypes[i].getSize(), COLUMNMETA_SIZE, byteArray);
            Convert.setStringValue(attrTypes[i].getAttrName(), COLUMNMETA_NAME, byteArray);
            RID rid = this.insertRecord(byteArray);
            if (rid == null) {
                PageId pageId = new PageId(this.getCurPage().pid);
                PageId nextPageId = new PageId(this.getNextPage().pid);
                HFPage hfPage = new HFPage();
                while (nextPageId.pid != INVALID_PAGE && rid == null) {
                    pageId.pid = nextPageId.pid;
                    pinPage(pageId, hfPage);
                    rid = hfPage.insertRecord(byteArray);
                    nextPageId.pid = hfPage.getNextPage().pid;
                    if (rid != null)
                        unpinPage(pageId, true);
                    else
                        unpinPage(pageId, false);
                }
                if (rid == null) {
                    HFPage page = new HFPage();
                    nextPageId = newPage(page);
                    pinPage(pageId, hfPage);
                    hfPage.setNextPage(nextPageId);
                    page.init(nextPageId, page);
                    page.setPrevPage(pageId);
                    rid = page.insertRecord(byteArray);
                    unpinPage(pageId, true);
                    unpinPage(nextPageId, true);
                    if (rid == null) {
                        throw new ColumnarMetaInsertException(null, "Columnar: Not able to insert meta info");
                    }
                }
            }
        }
    }


    public RID setIndex(IndexInfo info)
            throws IOException,
            ColumnarMetaInsertException,
            HFBufMgrException,
            ColumnarNewPageException {
        int infoLength = getlengthforIndexInfo(info);
        byte[] byteInfo = new byte[infoLength];
        Convert.setShortValue((short) info.getColumnNumber(), INDEXMATA_COLUMNID, byteInfo);
        Convert.setShortValue((short) info.getIndextype().indexType, INDEXMATA_INDEXTYPE, byteInfo);
        Convert.setStringValue(info.getFileName(), INDEXMATA_FILENAME, byteInfo);
        if (info.getValue().getValueType() == 1) {
            Convert.setIntValue((Integer) info.getValue().getValue(), INDEXMATA_VALUE, byteInfo);
        } else {
            Convert.setStringValue((String) info.getValue().getValue(), INDEXMATA_VALUE, byteInfo);
        }
        RID rid = this.insertRecord(byteInfo);
        if (rid == null) {
            PageId pageId = new PageId(this.getCurPage().pid);
            PageId nextPageId = new PageId(this.getNextPage().pid);
            HFPage hfPage = new HFPage();
            while (nextPageId.pid != INVALID_PAGE && rid == null) {
                pageId.pid = nextPageId.pid;
                pinPage(pageId, hfPage);

                rid = hfPage.insertRecord(byteInfo);
                nextPageId.pid = hfPage.getNextPage().pid;
                if (rid != null)
                    unpinPage(pageId, true);
                else
                    unpinPage(pageId, false);
            }
            if (rid == null) {
                HFPage page = new HFPage();
                nextPageId = newPage(page);
                pinPage(pageId, hfPage);
                hfPage.setNextPage(nextPageId);
                page.init(nextPageId, new Page());
                page.setPrevPage(pageId);
                rid = page.insertRecord(byteInfo);
                unpinPage(pageId, true);
                unpinPage(nextPageId, true);
                if (rid == null) {
                    throw new ColumnarMetaInsertException(null, "Columnar: Not able to insert meta info");
                }
            }
        }
        setCounter(++counter);
        return rid;
    }

    /*
     * get the length of the Index record
     * @param- IndexInfo contains the index of information
     * @return return integer- the length
     */
    private int getlengthforIndexInfo(IndexInfo info) {
        int length = 0;
        if (info.getValue().getValueType() == 1) {
            length += 4;
        } else if (info.getValue().getValueType() == 2) {
            length += 50;
            //to-do change the size
        } else if (info.getValue().getValueType() == -1) {
            //do nothing
        }
        length += 54; //2+2+50
        return length;
    }






    /*
     * function returns the columns info
     * reading from the page
     * @return array of AttrType(Column info)
     */

    public AttrType[] getColumns() throws IOException
            , HFBufMgrException, InvalidSlotNumberException {

        int countRecords = getColumnCount();
        AttrType[] attrTypes = new AttrType[countRecords];
        PageId pageId = new PageId(this.headerPageId.pid);
        HFPage page = new HFPage();
        PageId nextPageId;
        RID prevRID = null;
        pinPage(pageId, page);

        for (int i = 0; i < countRecords; i++) {
            RID rid = null;

            while (rid == null && pageId.pid != INVALID_PAGE) {
                if (prevRID == null) {
                    rid = page.firstRecord();
                } else {
                    rid = page.nextRecord(prevRID);
                    if (rid == null) {
                        nextPageId = page.getNextPage();
                        unpinPage(pageId, false);
                        pageId = nextPageId;
                        if (pageId.pid != INVALID_PAGE)
                            pinPage(pageId, page);
                    }
                }
                prevRID = rid;
            }

            attrTypes[i] = convertAttrByteInfo(page.getDataAtSlot(rid));
        }
        unpinPage(pageId, false);
        return attrTypes;
    }

    /*
     * convert byte[] to attrtype
     */
    private IndexInfo convertIndexByteInfo(byte[] byteinfo)
            throws IOException {
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.setColumnNumber(Convert.getShortValue(COLUMNMETA_COLUMNID, byteinfo));
        indexInfo.setIndexType(new IndexType(Convert.getShortValue(COLUMNMETA_ATTR, byteinfo)));
        indexInfo.setFileName(Convert.getStringValue(COLUMNMETA_SIZE, byteinfo, 50));
        indexInfo.setValue(new IntegerValue(Convert.getIntValue(COLUMNMETA_NAME, byteinfo)));

        return indexInfo;
    }


//    final private int INDEXMATA_COLUMNID = 0;
//    final private int INDEXMATA_INDEXTYPE = 2;
//    final private int INDEXMATA_FILENAME = 4;
//    final private int INDEXMATA_VALUE = 54;

    /*
     * function gets the index info from the meta-data file
     * it will  be used for Btree index
     * @param columnNum - columnId
     * @param indType - type of indexing
     * return type is record id
     */

    public IndexInfo getIndex(int columnNum, IndexType indType)
            throws IOException,
            HFBufMgrException,
            InvalidSlotNumberException {
        int indexCount = getCounter();
        int countRecords = getColumnCount();
        PageId pageId = new PageId(this.headerPageId.pid);
        HFPage page = new HFPage();
        PageId nextPageId;
        RID prevRID = null;
        IndexInfo info;
        pinPage(pageId, page);

        for (int i = 0; i < indexCount + countRecords; i++) {
            RID rid = null;

            while (rid == null && pageId.pid != INVALID_PAGE) {
                if (prevRID == null) {
                    rid = page.firstRecord();
                } else {
                    rid = page.nextRecord(prevRID);
                    if (rid == null) {
                        nextPageId = page.getNextPage();
                        unpinPage(pageId, false);
                        pageId = nextPageId;
                        if (pageId.pid != INVALID_PAGE)
                            pinPage(pageId, page);
                    }
                }
                prevRID = rid;
            }
            if (i >= countRecords) {
                info = convertIndexByteInfo(page.getDataAtSlot(rid));
                if (info.getColumnNumber() == columnNum && info.getIndextype() == indType) {
                    unpinPage(pageId, false);
                    return info;
                }
            }
        }
        unpinPage(pageId, false);
        return null;
    }

    /*
     * function gets the index info from the meta-data file
     * it will  be used for Bitmap index
     * @param columnNum - columnId
     * @param value - value of the column that indexing is applied
     * @param indType - type of indexing
     * return type is record id
     */
    public IndexInfo getIndex(int columnNum, ValueClass value, IndexType indType)
            throws IOException,
            HFBufMgrException,
            InvalidSlotNumberException {
        int countRecords = getColumnCount();
        PageId pageId = new PageId(this.headerPageId.pid);
        HFPage page = new HFPage();
        PageId nextPageId;
        RID prevRID = null;
        IndexInfo info;
        pinPage(pageId, page);

        for (int i = 0; i < getCounter(); i++) {
            RID rid = null;

            while (rid == null && pageId.pid != INVALID_PAGE) {
                if (prevRID == null) {
                    rid = page.firstRecord();
                } else {
                    rid = page.nextRecord(prevRID);
                    if (rid == null) {
                        nextPageId = page.getNextPage();
                        unpinPage(pageId, false);
                        pageId = nextPageId;
                        if (pageId.pid != INVALID_PAGE)
                            pinPage(pageId, page);
                    }
                }
                prevRID = rid;
            }
            if (i >= countRecords) {
                info = convertIndexByteInfo(page.getDataAtSlot(rid));
                if (info.getColumnNumber() == columnNum && info.getIndextype() == indType && info.getValue() == value) {
                    unpinPage(pageId, false);
                    return info;
                }
            }
        }
        unpinPage(pageId, false);
        return null;

    }

    /*
     * convert byte[] to attrtype
     */
    private AttrType convertAttrByteInfo(byte[] byteinfo)
            throws IOException {
        AttrType attrType = new AttrType();
        attrType.setColumnId(Convert.getShortValue(COLUMNMETA_COLUMNID, byteinfo));
        attrType.setAttrType(Convert.getShortValue(COLUMNMETA_ATTR, byteinfo));
        attrType.setSize(Convert.getShortValue(COLUMNMETA_SIZE, byteinfo));
        attrType.setAttrName(Convert.getStringValue(COLUMNMETA_NAME, byteinfo, 50));

        return attrType;
    }
    /*
     * function to return info about one column
     * @  return AttrType
     *
     */

    public AttrType getColumn(int i) throws IOException
            , InvalidSlotNumberException, HFBufMgrException {
        //TODO: Need to be optimzed
        return getColumns()[i];
    }


    public void setCounter(int indexColunter)
            throws IOException {

        Convert.setIntValue(indexColunter, PREV_PAGE, data);
    }


    /**
     * @return page number of next page
     * @throws IOException I/O errors
     */
    public int getCounter()
            throws IOException {
        int indexColunter = Convert.getIntValue(PREV_PAGE, data);
        return indexColunter;
    }

    /*
     * function to return number of columns
     * @  return AttrType
     *
     */
    public int getColumnCount() throws IOException {
        return this.getType();
    }

    private PageId newPage(Page page) throws ColumnarNewPageException {
        try {
            return SystemDefs.JavabaseBM.newPage(page, 1);
        } catch (Exception e) {
            throw new ColumnarNewPageException(null, "Columnar: Not able to get a new page for header");
        }
    }

    private PageId getFileEntry(String filename) throws HFDiskMgrException {
        PageId tmpId = new PageId();
        try {
            tmpId = SystemDefs.JavabaseDB.getFileEntry(filename);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: get_file_entry() failed");
        }

        return tmpId;

    } // end of get_file_entry

    private void addFileEntry(String filename, PageId pageno) throws HFDiskMgrException {

        try {
            SystemDefs.JavabaseDB.addFileEntry(filename, pageno);
        } catch (Exception e) {
            throw new HFDiskMgrException(e, "Heapfile.java: add_file_entry() failed");
        }

    } // end of add_file_entry

    private PageId newPage(Page page, int num)
            throws HFBufMgrException {

        PageId tmpId = new PageId();

        try {
            tmpId = SystemDefs.JavabaseBM.newPage(page, num);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: newPage() failed");
        }

        return tmpId;

    }

    private void unpinPage(PageId pageno, boolean dirty) throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: in Column Header, unpinPage() failed");
        }

    }

    private void pinPage(PageId pageId, Page page) throws HFBufMgrException {
        try {
            SystemDefs.JavabaseBM.pinPage(pageId, page, false);
        } catch (Exception e) {
            throw new HFBufMgrException(e, "Heapfile.java: in Column Header, pinPage() failed");
        }
    }

    public String getHdrFile() {
        return hdrFile;
    }

    public void setHdrFile(String hdrFile) {
        this.hdrFile = hdrFile;
    }

    public PageId getHeaderPageId() {
        return headerPageId;
    }

    public void setHeaderPageId(PageId headerPageId) {
        this.headerPageId = headerPageId;
    }
}
