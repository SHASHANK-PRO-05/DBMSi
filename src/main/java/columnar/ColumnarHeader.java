package columnar;

import java.io.IOException;

import diskmgr.FileNameTooLongException;
import diskmgr.Page;
import global.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.HFPage;


public class ColumnarHeader extends HFPage {
    String hdrFile;
    private int COLUMNMETA_COLUMNID = 0;
    private int COLUMNMETA_ATTR = 2;
    private int COLUMNMETA_SIZE = 4;
    private int COLUMNMETA_NAME = 6;


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

        if (name.length() >= 48)
            throw new FileNameTooLongException(null, "FILENAME: file name is too long");
        hdrFile = name + ".h";
        PageId hdrPageNo = getFileEntry(hdrFile);
        if (hdrPageNo == null) {
            hdrPageNo = newPage(this, 1);
            if (hdrPageNo == null)
                throw new ColumnHeaderCreateException(null, "can't allocate new page");
            addFileEntry(hdrFile, hdrPageNo);
            init(hdrPageNo, this);
            PageId pageId = new PageId(INVALID_PAGE);
            setType((short) numColumns);
            init(type);
            unpinPage(hdrPageNo, true /*dirty*/);
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
                    page.init(nextPageId, new Page());
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

}
