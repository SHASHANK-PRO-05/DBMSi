package testutils;

import columnar.ColumnClassCreationException;
import columnar.ColumnarFile;
import global.AttrType;
import global.Convert;
import global.SystemDefs;
import heap.HFDiskMgrException;

import java.io.IOException;
import java.util.Arrays;

public class BaseTest implements ITestConstants {

    protected ColumnarFile columnarFile = null;
    AttrType[] attrTypes = null;
    short[] sizes = null;

    protected void initDatabase(int numOfColumns, AttrType attrType)
            throws HFDiskMgrException, IOException, ColumnClassCreationException {
        attrTypes = new AttrType[numOfColumns];
        sizes = new short[numOfColumns];

        for (int i = 0; i < numOfColumns; i++) {
            attrTypes[i] = new AttrType();
            attrTypes[i].setColumnId(i);
            attrTypes[i].setSize(attrType.getSize());
            attrTypes[i].setAttrType(attrType.getAttrType());
            attrTypes[i].setAttrName("Column" + i);
            sizes[i] = (short) attrType.getSize();
        }

        SystemDefs systemDefs = new SystemDefs(dbName, numOfPages, bufPoolSize, "LRU");
        columnarFile = new ColumnarFile(employeeColumnarFile, numOfColumns, attrTypes);
    }

    protected void insertDummyData() throws Exception {
        insertDummyData(1000);
    }

    protected void insertDummyData(int numOfRows) throws Exception {
        int[] row = new int[attrTypes.length];
        for (int i = 0; i < numOfRows; i++) {
            for (int j = 0; j < attrTypes.length; j++) {
                row[j] = i % 2 == 0 ? 25 : 75;
            }

            columnarFile.insertTuple(Convert.intAtobyteA(row));
        }

        System.out.println("Insertion completed");
    }
}
