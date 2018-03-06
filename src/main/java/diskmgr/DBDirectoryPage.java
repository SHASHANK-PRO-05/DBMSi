package diskmgr;

import java.io.IOException;

public class DBDirectoryPage extends DBHeaderPage {
    public DBDirectoryPage() {
        super();
    }

    public DBDirectoryPage(Page page)
            throws IOException {
        super(page, DIR_PAGE_USED_BYTES);
    }
}