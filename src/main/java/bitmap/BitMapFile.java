package bitmap;

import java.io.IOException;


import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.ColumnarFile;
import diskmgr.Page;
import global.PageId;
import global.SystemDefs;
import global.ValueClass;


public class BitMapFile {


    private BitMapHeaderPage headerPage;
    private PageId headerPageId;
    private String fileName;
    private ColumnarFile columnarFile;


    /**
     * Access method to data member.
     *
     * @return Return a BitMapHeaderPage object that is the header page
     * of this bitmap file.
     */
    public BitMapHeaderPage getHeaderPage() {
        return headerPage;
    }


    private PageId getFileEntry(String fileName) throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.getFileEntry(fileName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    private Page pinPage(PageId pageId) throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageId, page, false/*Rdisk*/);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }


    private void addFileEntry(String fileName, PageId pageId)
            throws AddFileEntryException {
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

    private void deleteFileEntry(String fileName)
            throws DeleteFileEntryException {
        try {
            SystemDefs.JavabaseDB.deleteFileEntry(fileName);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DeleteFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageId, boolean dirty)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageId, dirty);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }


    public BitMapFile(String fileName) throws GetFileEntryException,
            PinPageException, ConstructPageException {
        headerPageId = getFileEntry(fileName);
        headerPage = new BitMapHeaderPage(headerPageId);
        this.fileName = fileName;
    }


    public BitMapFile(String fileName, ColumnarFile columnarFile, int columnNo, ValueClass value)
            throws GetFileEntryException,
            ConstructPageException, IOException, AddFileEntryException {

        headerPageId = getFileEntry(fileName);
        if (headerPageId == null) // file not exist
        {
            headerPage = new BitMapHeaderPage();
            headerPageId = headerPage.getPageId();
            addFileEntry(fileName, headerPageId);
            headerPage.setColumnIndex(columnNo);
            headerPage.setValue(value);
            this.columnarFile = columnarFile;
        } else {
            headerPage = new BitMapHeaderPage(headerPageId);
        }

        init();
        this.fileName = fileName;

    }


    /*
     * Initialize our bitMap
     */
    private void init() {
        //initialize a sequential scan on the
        //Scan scan = columnarFile.openColumnScan(1);
        //ValueClass value = headerpage.getValue()
        /*
         * get value of 1st tuple
         * ValueClass value = scan.getnext();
         *
         * check if the tuple match the value
         *
         * if yes then insert(1);
         * if no then position ++;		 */
        /*
         *
         */

        /*
         * while(scan.getNext()){
         * scan through the tuples
         *
         *
         * }
         */

    }


    public void close() throws PageUnpinnedException,
            InvalidFrameNumberException, HashEntryNotFoundException,
            ReplacerException {
        if (headerPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
            headerPage = null;
        }
    }

    public void destroyBitMapFile() {

    }

    public boolean Delete(int position) {
        return false;
    }

    public boolean Insert(int position) {
        return false;

    }


}