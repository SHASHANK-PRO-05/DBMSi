package diskmgr;

import global.Convert;
import global.GlobalConst;
import global.PageId;

import java.io.IOException;

public class DBHeaderPage implements GlobalConst
        , PageUsedBytes {
    protected static final int NEXT_PAGE = 0;
    protected static final int NUM_OF_ENTRIES = 4;
    protected static final int START_FILE_ENTRIES = 8;
    protected static final int SIZE_OF_FILE_ENTRY = 4 + MAX_NAME + 2;

    protected byte[] data;

    public DBHeaderPage() {

    }

    public DBHeaderPage(Page page, int pageUsedBytes)
            throws IOException {
        data = page.getPage();
        PageId pageId = new PageId();
        pageId.pid = INVALID_PAGE;
        setNextPage(pageId);
        int numEntries = (MINIBASE_PAGESIZE - pageUsedBytes) / SIZE_OF_FILE_ENTRY;
        setNumOfEntries(numEntries);
        for (int index = 0; index < numEntries; ++index) {
            initFileEntry(INVALID_PAGE, index);
        }
    }

    public void setNextPage(PageId pageId)
            throws IOException {
        Convert.setIntValue(pageId.pid, NEXT_PAGE, data);
    }

    private void initFileEntry(int empty, int entryNo)
            throws IOException {
        int position = START_FILE_ENTRIES + entryNo * SIZE_OF_FILE_ENTRY;
        Convert.setIntValue(empty, position, data);
    }

    protected void setNumOfEntries(int numEntries)
            throws IOException {
        Convert.setIntValue(numEntries, NUM_OF_ENTRIES, data);
    }

    public PageId getNextPage() throws IOException {
        PageId nextPage = new PageId();
        nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
        return nextPage;
    }

    public int getNumOfEntries()
            throws IOException {
        return Convert.getIntValue(NUM_OF_ENTRIES, data);
    }

    public void setFileEntry(PageId pageId, String fname, int entryNo)
            throws IOException {
        int position = START_FILE_ENTRIES + entryNo * SIZE_OF_FILE_ENTRY;
        Convert.setIntValue(pageId.pid, position, data);
        Convert.setStringValue(fname, position + 4, data);
    }

    public String getFileEntry(PageId pageNo, int entryNo)
            throws IOException {
        int position = START_FILE_ENTRIES + entryNo * SIZE_OF_FILE_ENTRY;
        pageNo.pid = Convert.getIntValue(position, data);
        return (Convert.getStringValue(position + 4, data, MAX_NAME + 2));
    }


    public void openPage(Page page) {
        data = page.getPage();
    }
}
