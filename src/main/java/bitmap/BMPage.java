package bitmap;

import java.io.*;

import global.Convert;
import global.PageId;
import global.GlobalConst;

import diskmgr.Page;


public class BMPage extends Page implements GlobalConst {
    //start of my bit map in data file.
    public static final int DPFIXED = 2 * 2 + 3 * 4;

    //header
    public static final int COUNTER = 0;
    public static final int FREE_SPACE = 2;
    public static final int PREV_PAGE = 4;
    public static final int NEXT_PAGE = 8;
    public static final int CUR_PAGE = 12;

    public static final int availableMap = (MINIBASE_PAGESIZE - DPFIXED) * 8;
    /*
     * number of bits set
     */

    private short count;

    public int getStartByte() {
        return DPFIXED;
    }

    /**
     * number of bytes free in data[]
     */
    private short freeSpace;

    /**
     * backward pointer to data page
     */
    private PageId prevPage = new PageId();

    /**
     * forward pointer to data page
     */
    private PageId nextPage = new PageId();


    public int getAvailableMap() {
        return availableMap;
    }

    /**
     * page number of this page
     */
    protected PageId curPage = new PageId();


    /**
     * Default constructor
     */

    public BMPage() {
    }

    /**
     * Constructor of class BMPage
     * open a BMPage and make this BMpage point to the given page
     *
     * @param page the given page in Page type
     */

    public BMPage(Page page) {
        data = page.getPage();
    }


    /**
     * Constructor of class BMPage
     * initialize a new page
     *
     * @param pageNo the page number of a new page to be initialized
     * @param apage  the Page to be initialized
     * @throws IOException I/O errors
     * @see Page
     */
    public void init(PageId pageNo, Page apage)
            throws IOException {
        data = apage.getPage();
        count = 0;                // no slots in use
        Convert.setShortValue(count, COUNTER, data);
        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);
        nextPage.pid = prevPage.pid = INVALID_PAGE;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
        freeSpace = (short) (MINIBASE_PAGESIZE - DPFIXED);    // amount of space available
        Convert.setShortValue(freeSpace, FREE_SPACE, data);
    }

    public int availableSpace() throws IOException {
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        return (freeSpace);
    }

    public short getCount() throws IOException {
        count = Convert.getShortValue(COUNTER, data);
        return count;
    }

    /**
     * @return byte array
     */
    public byte[] getHFPageArray() {
        return data;

    }

    /**
     * @return PageId of previous page
     * @throws IOException I/O errors
     */
    public PageId getPrevPage() throws IOException {
        prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
        return prevPage;
    }

    /**
     * sets value of prevPage to pageNo
     *
     * @param pageNo page number for previous page
     * @throws IOException I/O errors
     */
    public void setPrevPage(PageId pageNo) throws IOException {
        prevPage.pid = pageNo.pid;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
    }


    /**
     * @return page number of next page
     * @throws IOException I/O errors
     */
    public PageId getNextPage() throws IOException {
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        return nextPage;
    }

    /**
     * sets value of nextPage to pageNo
     *
     * @param pageNo page number for next page
     * @throws IOException I/O errors
     */
    public void setNextPage(PageId pageNo) throws IOException {
        nextPage.pid = pageNo.pid;
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
    }

    /**
     * @return page number of current page
     * @throws IOException I/O errors
     */
    public PageId getCurPage() throws IOException {
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        return curPage;
    }

    /**
     * sets value of curPage to pageNo
     *
     * @param pageNo page number for current page
     * @throws IOException I/O errors
     */
    public void setCurPage(PageId pageNo) throws IOException {
        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);
    }

    /**
     * Constructor of class BMPage
     * open a existed bmpage
     *
     * @param apage a page in buffer pool
     */
    public void openBMpage(Page apage) {
        data = apage.getPage();
    }


    /**
     * Determining if the page is empty
     *
     * @return true if the HFPage is has no records in it, false otherwise
     * @throws IOException I/O errors
     */
    public boolean empty() throws IOException {
        boolean isEmpty = false;
        count = Convert.getShortValue(COUNTER, data);
        if (count == 0) {
            isEmpty = true;
        }
        return isEmpty;
    }


    /**
     * Dump contents of a page
     *
     * @throws IOException I/O errors
     */
    public void dumpPage() throws IOException {

        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        freeSpace = Convert.getShortValue(FREE_SPACE, data);
        count = Convert.getShortValue(COUNTER, data);

        System.out.println("dumpPage");
        System.out.println("curPage= " + curPage.pid);
        System.out.println("nextPage= " + nextPage.pid);
        System.out.println("freeSpace= " + freeSpace);
        System.out.println("slotCnt= " + count);
        int i; //for iteration
        for (i = 0; i < count; i++) {
            // code to traverse through the code
        }
    }


    /**
     * Write the rid into a byte array at offset
     *
     * @param ary the specified byte array
     * @throws IOException I/O errors
     */
    public void writeToByteArray(byte[] ary) throws IOException {

        Convert.setIntValue(count, COUNTER, ary);
        Convert.setIntValue(freeSpace, FREE_SPACE, ary);
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);

        // write now bit map the, I do not know the usage
        //now so leaving it blank

    }


}
