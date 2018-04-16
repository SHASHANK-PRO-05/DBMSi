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
import java.util.PriorityQueue;

public class Test {
    SystemDefs systemDefs;
    ColumnarFile columnarFile;
    String relName = "Employee1";
    AttrType[] attrTypes;
    FldSpec[] fldSpecs;
    CondExpr[] condExprs;
    IndexType[] indexTypes;

    @Before
    public void start() throws Exception {
        systemDefs = new SystemDefs("Minibase.min", 0, 400, "LRU");
        columnarFile = new ColumnarFile(relName);
        attrTypes = columnarFile.getColumnarHeader().getColumns();
        fldSpecs = new FldSpec[attrTypes.length];
        for (int i = 0; i < fldSpecs.length; i++) {
            fldSpecs[i] = new FldSpec(new RelSpec(0), i);
        }
    }

    @org.junit.Test
    public void Test() throws Exception {
        condExprs = new CondExpr[3];
        // Condition 0: ( A >=  OR B ==  )
        /*
         Column 0 is A
         Column 1 is B in Attrtypes
         */
        condExprs[0] = new CondExpr();
        condExprs[0].operand1.symbol = fldSpecs[0];
        condExprs[0].op = new AttrOperator(AttrOperator.aopEQ);
        condExprs[0].operand2.string = "Montana";
        condExprs[0].next = new CondExpr();
        condExprs[0].next.operand1.symbol = fldSpecs[1];
        condExprs[0].next.op = new AttrOperator(AttrOperator.aopEQ);
        condExprs[0].next.operand2.string = "Montana";


        // condition 1: C >=
        condExprs[1] = new CondExpr();
        condExprs[1].next = null;
        condExprs[1].operand1.symbol = fldSpecs[2];
        condExprs[1].op = new AttrOperator(AttrOperator.aopGE);
        condExprs[1].operand2.integer = 6;


        // Condition 2: D == 7
        condExprs[2] = new CondExpr();
        condExprs[2].next = null;
        condExprs[2].operand1.symbol = fldSpecs[3];
        condExprs[2].op = new AttrOperator(AttrOperator.aopLE);
        condExprs[2].operand2.integer = 3;
        parseIterators();
        ColumnarIndexScan columnarIndexScan = new ColumnarIndexScan(relName,
                null, indexTypes, null, attrTypes, null, -1, -1, fldSpecs, condExprs);
        AttrType[] projectionBreak = attrTypes;
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

    /**
     * This can be made generic
     */
    public void parseIterators() throws Exception {
        PriorityQueue<Integer>[] priorityQueues = new PriorityQueue[attrTypes.length];
        indexTypes = new IndexType[attrTypes.length];

        for (int i = 0; i < condExprs.length; i++) {
            CondExpr condExpr = condExprs[i];
            while (condExpr != null) {
                int offset = condExpr.operand1.symbol.offset;
                if (priorityQueues[offset] == null) {
                    priorityQueues[offset] = new PriorityQueue<>();
                }
                if (condExpr.op.attrOperator == AttrOperator.aopEQ) {
                    //In equality Bitmap will get the priority
                    if (columnarFile.getColumnarHeader().getParticularTypeIndex(offset, new IndexType(IndexType.BitMapIndex)).size() != 0) {
                        priorityQueues[offset].add(IndexType.BitMapIndex);
                    } else {
                        priorityQueues[offset].add(IndexType.ColumnScan);
                    }
                } else {
                    // Possible range queries should have B+ tree as priority
                    if (columnarFile.getColumnarHeader().getParticularTypeIndex(offset, new IndexType(IndexType.B_Index)).size() != 0) {
                        priorityQueues[offset].add(IndexType.B_Index);
                    } else if (columnarFile.getColumnarHeader().getParticularTypeIndex(offset, new IndexType(IndexType.BitMapIndex)).size() != 0) {
                        priorityQueues[offset].add(IndexType.BitMapIndex);
                    } else {
                        priorityQueues[offset].add(IndexType.ColumnScan);
                    }
                }
                condExpr = condExpr.next;
            }
        }
        for (int i = 0; i < indexTypes.length; i++) {
            if (priorityQueues[i] != null) {
                indexTypes[i] = new IndexType(priorityQueues[i].poll());
            }
        }
    }


}
