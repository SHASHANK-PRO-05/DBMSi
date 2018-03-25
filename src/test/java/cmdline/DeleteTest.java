package cmdline;

import columnar.ByteToTuple;
import columnar.TupleScan;
import diskmgr.DiskMgrException;
import diskmgr.FileEntryNotFoundException;
import global.*;
import heap.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import testutils.BaseTest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class DeleteTest extends BaseTest {

    @Before
    public void setupDatabase() throws Exception {
        AttrType numType = new AttrType();
        numType.setSize((short) 4);
        numType.setAttrType(AttrType.attrInteger);
        initDatabase(5, numType);
        insertDummyData();
        System.out.println(columnarFile.getTupleCount());
        SystemDefs.JavabaseBM.flushAllPages();
    }

    @Test
    public void deleteUsingColumnarScan() throws Exception {
        String[] argv = {dbName, employeeColumnarFile, "Column2", "<", "50", Integer.toString(bufPoolSize), "COLUMNSCAN", Boolean.toString(true)};
        Delete.main(argv);

        AttrType[] attrTypes = columnarFile.getColumnarHeader().getColumns();
        int[] columns = new int[attrTypes.length];
        RID[] rids = new RID[attrTypes.length];
        for (int i = 0; i < attrTypes.length; i++) {
            columns[i] = attrTypes[i].getColumnId();
            rids[i] = new RID();
        }

        TupleScan tupleScan = new TupleScan(columnarFile, columns);

        TID tid = new TID(attrTypes.length, 0, rids);

        Tuple tuple = tupleScan.getNext(tid);
        long resultingTupleCount = 0;

        while (tuple != null) {
            ByteToTuple byteToTuple = new ByteToTuple(columnarFile.getColumnarHeader().getColumns());
            ArrayList<byte[]> arrayList = byteToTuple.setTupleBytes(tuple.getTupleByteArray());

            if (Convert.getIntValue(0, arrayList.get(2)) < 50) {
                System.out.println(tid.getPosition());
                System.out.println(Arrays.toString(tuple.getTupleByteArray()));
            }

            resultingTupleCount += 1;

            tuple = tupleScan.getNext(tid);
        }

        SystemDefs.JavabaseBM.pinPage(columnarFile.getColumnarHeader().getHeaderPageId(), columnarFile.getColumnarHeader(), false);
        SystemDefs.JavabaseBM.unpinPage(columnarFile.getColumnarHeader().getHeaderPageId(), false);

        long currentTupleCount = columnarFile.getColumnarHeader().getReccnt();

        assert resultingTupleCount == currentTupleCount;
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
