package bitmap;

import diskmgr.Page;
import global.Convert;
import global.PageId;
import global.SystemDefs;
import global.ValueClass;

import java.io.IOException;


public class BitMapHeaderPage extends Page {
    public static final int PREV_PAGE = 0; //What is the next page
    public static final int CURRENT_PAGE = 4; //Current PageId
    public static final int NEXT_PAGE = 8; //Where the BMPage is going to start
    public static final int COLUMN_INDEX = 12; //Column index number
    public static final int VALUE_TYPE = 14; //ValueClass Type
    public static final int BM_FIX = 16; //Bytes reserved

    private PageId prevPage = new PageId(-1);
    private PageId currPage = new PageId(-1);
    private PageId nextPage = new PageId(-1);
    private short columnIndex = -1;
    private short valueType = -1;

    public BitMapHeaderPage() throws ConstructPageException {
        super();
        Page page = new Page();

        try {
            PageId pageId = SystemDefs.JavabaseBM.newPage(page, 1);
            if (pageId == null || pageId.pid == INVALID_PAGE)
                throw new ConstructPageException(null, "new page failed");
            this.init(pageId, page);
        } catch (Exception e) {
            throw new ConstructPageException(e, "construct header page failed");
        }
    }


    public void init(PageId pageId, Page page)
            throws IOException {
        data = page.getPage();
        currPage.pid = pageId.pid;
        Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
        Convert.setIntValue(currPage.pid, CURRENT_PAGE, data);
        Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
        Convert.setShortValue(columnIndex, COLUMN_INDEX, data);
        Convert.setShortValue(valueType, VALUE_TYPE, data);
    }

    public PageId getPrevPage() throws IOException {
        prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
        return prevPage;
    }

    public void setPrevPage(PageId prev) throws IOException {
        Convert.setIntValue(prev.pid, PREV_PAGE, data);
        this.prevPage = prevPage;
    }

    public PageId getCurrPage() throws IOException {
        currPage.pid = Convert.getIntValue(CURRENT_PAGE, data);
        return currPage;
    }

    public void setCurrPage(PageId curr) throws IOException {
        Convert.setIntValue(curr.pid, CURRENT_PAGE, data);
        this.currPage = currPage;
    }

    public PageId getNextPage() throws IOException {
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        return nextPage;
    }

    public void setNextPage(PageId next) throws IOException {
        Convert.setIntValue(next.pid, NEXT_PAGE, data);
        this.nextPage = nextPage;
    }

    public short getColumnIndex() throws IOException {
        columnIndex = Convert.getShortValue(COLUMN_INDEX, data);
        return columnIndex;
    }

    public void setColumnIndex(short index) throws IOException {
        Convert.setShortValue(index, COLUMN_INDEX, data);
        this.columnIndex = columnIndex;
    }

    public short getValueType() throws IOException {
        valueType = Convert.getShortValue(VALUE_TYPE, data);
        return valueType;
    }

    public void setValueType(short value) throws IOException {
        Convert.setShortValue(value, VALUE_TYPE, data);
        this.valueType = valueType;
    }

    public BitMapHeaderPage(PageId pageId)
            throws ConstructPageException {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageId, this, false);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pinpage failed");
        }
    }
}