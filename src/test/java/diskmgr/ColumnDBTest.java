package diskmgr;


import global.Convert;
import global.PageId;
import global.SystemDefs;
import org.junit.*;

import java.io.IOException;
import java.util.Random;

public class ColumnDBTest {
    private PageId runStart = new PageId();

    SystemDefs sysdef;
    String dbPath = "/tmp/Minibase.min";

    @Before
    public void beforeTests() {
        SystemDefs sysdef = new SystemDefs(dbPath, 8193,
                100, "Clock");
    }

    @Test
    public void AddFileEntries() throws Exception {

        PageId pageId = new PageId();
        pageId.pid = 0;
        System.out.print("  - Add some file entries\n");
        for (int i = 0; i < 6; ++i) {
            SystemDefs.JavabaseDB.allocatePage(pageId, 1);
            String fileName = "file" + i;
            SystemDefs.JavabaseDB.addFileEntry(fileName, pageId);
            SystemDefs.JavabaseDB.allocatePage(runStart, 30);
        }
        for (int i = 0; i < 20; ++i) {
            String writeStr = "A" + i;
            byte[] data = new byte[2 * writeStr.length()];
            Convert.setStringValue(writeStr, 0, data);
            Page page = new Page(data);
            SystemDefs.JavabaseDB.writePage(new PageId(runStart.pid + i), page);
        }
        SystemDefs.JavabaseDB.deallocatePage(new PageId(runStart.pid + 20), 10);
    }

    @Test
    public void ReadFileEntries() throws Exception {
        for (int i = 0; i < 3; ++i) {
            String name = "file" + i;
            SystemDefs.JavabaseDB.deleteFileEntry(name);
        }
    }
}
