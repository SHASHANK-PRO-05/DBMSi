package diskmgr;

import global.GlobalConst;
import global.PageId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ColumnDB implements GlobalConst {
    private static final int bitsPerPage = MINIBASE_PAGESIZE * 8;
    private RandomAccessFile filePointer;
    private int numPages;
    private String fName;

    public ColumnDB() {

    }

    public void openDB(String fName, int numPages) throws IOException {
        this.fName = fName;
        this.numPages = numPages;

        //Just making sure the file is deleted
        File dbFile = new File(this.fName);
        dbFile.delete();


        this.filePointer = new RandomAccessFile(this.fName, "rw");
        this.filePointer.seek(this.numPages * MINIBASE_PAGESIZE - 1);
        this.filePointer.writeByte(0);

        Page page = new Page();
        PageId pageId = new PageId();
        pageId.pid = 0;
    }

    private void pinPage(PageId pageNo, Page page, boolean dirty) {

    }
}
