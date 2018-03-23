package iterator;

import columnar.ColumnarFile;
import global.AttrOperator;
import global.AttrType;
import global.Convert;
import global.SystemDefs;
import heap.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IteratorsTest {
    @Before
    public void beforeAll() {

    }

    @Test
    public void ColumnarFileScanTest() throws Exception {
        SystemDefs systemDefs = new SystemDefs("Minibase.min", 0, 5000, "MRU");
        ColumnarFile columnarFile = new ColumnarFile("Employee");
        short[] s_sizes = new short[20];
        for (int i = 0; i < 20; i++) {
            s_sizes[i] = 12;
        }
        CondExpr[] condExprs = new CondExpr[2];
        condExprs[1] = null;
        condExprs[0] = new CondExpr();
        condExprs[0].op = new AttrOperator(AttrOperator.aopLE);
        condExprs[0].type1 = new AttrType(AttrType.attrSymbol);
        condExprs[0].type2 = new AttrType(AttrType.attrString);
        condExprs[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        condExprs[0].operand2.string = "AYTXAPT0HD";


        FldSpec[] projectList = new FldSpec[2];
        RelSpec relSpec = new RelSpec(RelSpec.outer);
        projectList[0] = new FldSpec(relSpec, 0);
        projectList[1] = new FldSpec(relSpec, 1);


        AttrType[] attrTypes = new AttrType[3];
        AttrType[] attrTypes1 = columnarFile.getColumnarHeader().getColumns();
        System.arraycopy(attrTypes1, 0, attrTypes, 0, 3);
        ColumnarFileScan columnarFileScan = new ColumnarFileScan("Employee"
                , attrTypes, s_sizes, 3, 2
                , projectList, condExprs);
        Tuple tuple = columnarFileScan.getNext();

        while (tuple != null) {
            int i = 0;
            for (int k = 0; k < projectList.length; k++) {

                String s = Convert.getStringValue(i, tuple.getTupleByteArray(), 12);
                System.out.print(s + " ");
                i = i + 12;
            }
            System.out.println();
            tuple = columnarFileScan.getNext();
        }
        columnarFileScan.close();
        SystemDefs.JavabaseBM.flushAllPages();
    }

    @After
    public void afterAll() {

    }
}
