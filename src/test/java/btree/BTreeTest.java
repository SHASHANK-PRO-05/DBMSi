package btree;

import columnar.ColumnarFile;
import columnar.TupleScan;
import global.*;
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
        SystemDefs systemDefs = new SystemDefs(dbPath, 20000, 10000, null);
        AttrType[] attrTypes = new AttrType[20];
        String[][] in = new String[10000][20];
        int[] sizes = new int[20];
        for (int i = 0; i < 20; i++) {
            attrTypes[i] = new AttrType();
            attrTypes[i].setColumnId(i);
            attrTypes[i].setSize((short) 12);
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
        String fileName = "Employee.1.btree";
        BTreeFile bTreeFile = new BTreeFile(fileName
                , attrTypes[1].getAttrType(), attrTypes[1].getSize(), 1);
        //Scan scan = new Scan(columnarFile, (short) 1);
        TupleScan tupleScan = new TupleScan(columnarFile);
        //initialization
        RID[] rids = new RID[20];
        for (int i = 0; i < 20; i++) rids[i] = new RID();
        TID tid = new TID(20, 0, rids);
        Tuple tuple = tupleScan.getNext(tid);


        tid.setPosition(0);
        int pos = 0;

        while (tuple != null) {
            StringValue stringValue = new StringValue(Convert
                    .getStringValue(0, tuple.getTupleByteArray(), attrTypes[3].getSize()));
            StringKey stringKey = new StringKey(stringValue.getValue());

            bTreeFile.insert(stringKey, tid);
            tuple = tupleScan.getNext(tid);
            pos++;
            tid.setPosition(pos);
        }
        //BT.printAllLeafPages(bTreeFile.getHeaderPage());
        bTreeFile.close();
        tupleScan.closeTupleScan();
        SystemDefs.JavabaseBM.flushAllPages();
    }

    @After
    public void destroy() {


    }
}