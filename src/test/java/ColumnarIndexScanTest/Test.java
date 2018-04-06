package ColumnarIndexScanTest;

import columnar.ByteToTuple;
import columnar.ColumnarFile;
import global.*;
import heap.Tuple;
import iterator.ColumnarIndexScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import org.junit.Before;

import java.util.ArrayList;

public class Test {
    SystemDefs systemDefs;

    @Before
    public void start() {
        systemDefs = new SystemDefs("Minibase.min", 0, 400, "LRU");
    }

    @org.junit.Test
    public void Test() throws Exception {
        String relName = "Employee1";
        IndexType[] indexTypes = new IndexType[2];
        indexTypes[0] = new IndexType(IndexType.ColumnScan);
        indexTypes[1] = new IndexType(IndexType.ColumnScan);
        ColumnarFile columnarFile = new ColumnarFile(relName);
        FldSpec[] fldSpecs = new FldSpec[3];
        fldSpecs[0] = new FldSpec(new RelSpec(0), 0);
        fldSpecs[1] = new FldSpec(new RelSpec(0), 2);
        fldSpecs[2] = new FldSpec(new RelSpec(0), 1);
        CondExpr[] condExprs = new CondExpr[2];
        condExprs[0] = new CondExpr();
        condExprs[0].op = new AttrOperator(AttrOperator.opRANGE);

        AttrType[] attrTypes = columnarFile.getColumnarHeader().getColumns();
        condExprs[0].operand2.stringRange = new String[2];
        condExprs[0].operand2.stringRange[0] = "A";
        condExprs[0].operand2.stringRange[1] = "Z";

        condExprs[0].next = new CondExpr();
        condExprs[0].next.op = new AttrOperator(AttrOperator.opRANGE);
        condExprs[0].next.operand2.stringRange = new String[2];
        condExprs[0].next.operand2.stringRange[0] = "Idaho";
        condExprs[0].next.operand2.stringRange[1] = "Singapore";

        condExprs[1] = new CondExpr();
        condExprs[1].op = new AttrOperator(AttrOperator.opRANGE);
        condExprs[1].operand2.integerRange = new int[2];
        condExprs[1].operand2.integerRange[0] = 0;
        condExprs[1].operand2.integerRange[1] = 4;
        condExprs[1].next = null;
        AttrType[] attrTypes1 = new AttrType[3];
        attrTypes1[0] = attrTypes[0];
        attrTypes1[1] = attrTypes[2];
        attrTypes1[2] = attrTypes[1];
        ColumnarIndexScan columnarIndexScan = new ColumnarIndexScan(relName, null, indexTypes, null
                , attrTypes1, null, -1, -1, fldSpecs, condExprs, false);
        AttrType[] projectionBreak = new AttrType[3];
        projectionBreak[0] = attrTypes[0];
        projectionBreak[1] = attrTypes[2];
        projectionBreak[2] = attrTypes[1];
        ByteToTuple byteToTuple = new ByteToTuple(projectionBreak);

        String ans2 = "Count";
        System.out.print(ans2);
        int temp2 = 25 - ans2.length();
        for (int j = 0; j < temp2; j++)
            System.out.print(" ");
        for (int i = 0; i < projectionBreak.length; i++) {
            String ans = projectionBreak[i].getAttrName();
            System.out.print(ans);
            int temp = 25 - ans.length();
            for (int j = 0; j < temp; j++)
                System.out.print(" ");

        }
        System.out.println();
        int counter = 1;

        Tuple tuple = columnarIndexScan.getNext();
        while (tuple != null) {
            ArrayList<byte[]> tuples = byteToTuple.setTupleBytes(tuple.getTupleByteArray());
            String ans1 = counter + "";
            System.out.print(ans1);
            int temp1 = 25 - ans1.length();
            for (int j = 0; j < temp1; j++)
                System.out.print(" ");

            counter++;
            for (int i = 0; i < projectionBreak.length; i++) {
                if (projectionBreak[i].getAttrType() == AttrType.attrString) {
                    String ans = Convert.getStringValue(0, tuples.get(i), projectionBreak[i].getSize());
                    System.out.print(ans);
                    int temp = 25 - ans.length();
                    for (int j = 0; j < temp; j++)
                        System.out.print(" ");
                } else {
                    int ans = Convert.getIntValue(0, tuples.get(i));
                    System.out.print(ans);
                    int temp = 25 - (ans + "").length();
                    for (int j = 0; j < temp; j++)
                        System.out.print(" ");
                }
            }
            System.out.println();
            tuple = columnarIndexScan.getNext();

        }

        columnarIndexScan.close();
        System.out.println("Tuples in the table now:" + columnarFile.getTupleCount());
        System.out.println("Write count: " + SystemDefs.pCounter.getwCounter());
        System.out.println("Read count: " + SystemDefs.pCounter.getrCounter());
        SystemDefs.JavabaseBM.flushAllPages();
        SystemDefs.JavabaseDB.closeDB();
        SystemDefs.JavabaseBM.flushAllPages();

    }
}
