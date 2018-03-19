package bitmap;

import columnar.ColumnarFile;
import columnar.ColumnarHeader;
import global.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class BitMapTest {
    @Before
    public void setup() {

    }

    private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public String randomAlphaNumeric(int count) {
        StringBuilder builder = new StringBuilder();
        while (count-- != 0) {
            int character = (int) (Math.random() * ALPHA_NUMERIC_STRING.length());
            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }
        return builder.toString();

    }

    @Test
    public void setupBitMapOperation() throws Exception {
        String dbPath = "Minibase.min";
        SystemDefs systemDefs = new SystemDefs(dbPath, 20000, 10, null);
        AttrType[] attrTypes = new AttrType[20];
        String[][] in = new String[10000][20];
        int[] sizes = new int[20];
        for (int i = 0; i < 20; i++) {
            attrTypes[i] = new AttrType();
            attrTypes[i].setColumnId(i);
            attrTypes[i].setSize((short)12);
            attrTypes[i].setAttrType(0);
            attrTypes[i].setAttrName("Column" + i);
            sizes[i] = 12;
        }
        ColumnarFile columnarFile = new ColumnarFile("Employee", 20, attrTypes);

        int min = Integer.MAX_VALUE;
        for (int i = 0; i < 10000; i++) {
            for (int j = 0; j < 20; j++) {
                if (i % 10 == 0)
                    in[i][j] = randomAlphaNumeric(10);
                else {
                    in[i][j] = "Shashank";
                }
            }
            try {
                columnarFile.insertTuple(Convert.stringToByteA(in[i], sizes));

            } catch (Exception e) {
                System.out.println();
            }
        }
        SystemDefs.JavabaseBM.flushAllPages();
        columnarFile.createBitMapIndex(3, new StringValue("Shashank"));
        SystemDefs.JavabaseBM.flushAllPages();
        BitMapFile bitMapFile = new BitMapFile("Employee.3.Shashank");
        SystemDefs.JavabaseBM.flushAllPages();
        BitMapOperations bitMapOperations = new BitMapOperations();
        SystemDefs.JavabaseBM.flushAllPages();
        bitMapFile.Delete(9999);
        SystemDefs.JavabaseBM.flushAllPages();

        bitMapFile.Insert(10000);
        SystemDefs.JavabaseBM.flushAllPages();

        bitMapOperations.getIndexedPostions(bitMapFile);
        SystemDefs.JavabaseBM.flushAllPages();

    }

    @After
    public void cleanUp() {

    }


}
