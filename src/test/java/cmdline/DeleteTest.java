package cmdline;

import diskmgr.DiskMgrException;
import diskmgr.FileEntryNotFoundException;
import global.AttrType;
import global.SystemDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import testutils.BaseTest;

import java.io.File;
import java.io.IOException;

public class DeleteTest extends BaseTest {

    @Before
    public void setupDatabase() throws Exception {
        SystemDefs systemDefs = new SystemDefs(dbName, numOfPages, bufPoolSize, "LRU");
        AttrType numType = new AttrType();
        numType.setSize((short) 4);
        numType.setAttrType(AttrType.attrInteger);
        initDatabase(5, numType);
        insertDummyData();
        SystemDefs.JavabaseBM.flushAllPages();
    }

    @Test
    public void deleteUsingColumnarScan() throws Exception {
        String[] argv = {dbName, employeeColumnarFile, "Column2", "<", "50", Integer.toString(bufPoolSize), "COLUMNSCAN", Boolean.toString(true)};
        Delete.main(argv);
    }

    @After
    public void cleanupDatabase() throws DiskMgrException, FileEntryNotFoundException, IOException {
        File file = new File(dbName);

        if (file.delete()) {
            System.out.println("Database cleaned up.");
        } else {
            System.out.println("Could not find " + dbName + " to delete.");
        }
    }
}
