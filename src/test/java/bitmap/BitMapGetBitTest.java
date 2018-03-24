package bitmap;

import columnar.ColumnClassCreationException;
import global.AttrType;
import global.SystemDefs;
import global.TID;
import heap.HFDiskMgrException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import testutils.BaseTest;

import java.io.File;
import java.io.IOException;

public class BitMapGetBitTest extends BaseTest {

    @Before
    public void setupDB() throws Exception {
        AttrType numType = new AttrType(AttrType.attrInteger);
        numType.setSize((short) 4);
        initDatabase(5, numType);
        insertDummyData();
        SystemDefs.JavabaseBM.flushAllPages();
    }

    @Test
    public void testBitSetAndGet() throws Exception {
        testForPosition(10);
    }

    @Test
    public void testBitSetAndGetForALargeNumber() throws Exception {
        testForPosition(10000);
    }

    void testForPosition(long position) throws Exception {
        TID tid = new TID(5, (int) position);
        columnarFile.markTupleDeleted(tid);

        boolean isTupleDeleted = columnarFile.isTupleDeletedAtPosition(position);

        assert isTupleDeleted;

        isTupleDeleted = columnarFile.isTupleDeletedAtPosition(position - 1);
        assert !isTupleDeleted;

        isTupleDeleted = columnarFile.isTupleDeletedAtPosition( position + 1);
        assert !isTupleDeleted;
    }

    @After
    public void cleanupDB() {
        File file = new File(dbName);

        if (file.delete()) {
            System.out.println("Database cleared.");
        } else {
            System.out.println("Couldn't find db file to clear.");
        }
    }
}
