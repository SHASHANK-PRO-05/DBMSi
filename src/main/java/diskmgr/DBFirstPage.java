package diskmgr;

import global.Convert;

import java.io.IOException;

public class DBFirstPage extends DBHeaderPage {

    protected static final int NUM_DB_PAGE = MINIBASE_PAGESIZE - 4;

    public DBFirstPage() {
        super();
    }

    public DBFirstPage(Page page)
            throws IOException {
        super(page, FIRST_PAGE_USED_BYTES);
    }

    public void setNumDBPages(int num)
            throws IOException {
        Convert.setIntValue(num, NUM_DB_PAGE, data);
    }

    public int getNumDBPages()
            throws IOException {
        return (Convert.getIntValue(NUM_DB_PAGE, data));
    }
}

