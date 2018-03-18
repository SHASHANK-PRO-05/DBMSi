package btree;

import columnar.ColumnarFile;
import global.*;
import heap.Scan;
import heap.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BTreeTest {
    @Before
    public void initialize() {

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
    public void test() throws Exception {
        String dbPath = "Minibase.min";
        SystemDefs systemDefs = new SystemDefs(dbPath, 20000, 10, null);
        AttrType[] attrTypes = new AttrType[20];
        String[][] in = new String[10000][20];
        int[] sizes = new int[20];
        for (int i = 0; i < 20; i++) {
            attrTypes[i] = new AttrType();
            attrTypes[i].setColumnId(i);
            attrTypes[i].setSize(12);
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
        String fileName = "Employee.3.btree";
        BTreeFile bTreeFile = new BTreeFile(fileName
                , attrTypes[3].getAttrType(), attrTypes[3].getSize(), 1);
        Scan scan = new Scan(columnarFile, (short) 1);
        RID rid = scan.getFirstRID();
        Tuple tuple = scan.getNext(rid);
        while (tuple != null) {
            StringValue stringValue = new StringValue(Convert
                    .getStringValue(0, tuple.getTupleByteArray(), attrTypes[3].getSize()));
            StringKey stringKey = new StringKey(stringValue.getValue());
            bTreeFile.insert(stringKey, rid);
            tuple = scan.getNext(rid);
        }
        BT.printAllLeafPages(bTreeFile.getHeaderPage());
        bTreeFile.close();
        SystemDefs.JavabaseBM.flushAllPages();
    }

    @After
    public void destroy() {


    }
}
