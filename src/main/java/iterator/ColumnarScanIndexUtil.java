package iterator;

import bitmap.BitMapFile;
import columnar.ColumnarFile;
import global.*;
import heap.Scan;
import heap.Tuple;

import java.util.ArrayList;

public class ColumnarScanIndexUtil extends LateMaterializationUtil {
    Scan scan;
    CondExpr condExpr;
    ColumnarFile columnarFile;
    private int position = -1;
    AttrType attrType;

    private ArrayList<BitMapFile> bitMapFilesToDelete = new ArrayList<BitMapFile>();

    public ColumnarScanIndexUtil(CondExpr condExpr
            , AttrType attrType
            , String relName) throws Exception {
        this.condExpr = condExpr;
        this.attrType = attrType;
        this.columnarFile = new ColumnarFile(relName);
        this.scan = new Scan(this.columnarFile
                , (short) attrType.getColumnId());
    }

    public int getNextPosition() throws Exception {
        RID rid = new RID();
        Tuple tuple = scan.getNext(rid);
        while (tuple != null) {
            position++;
            CondExpr tempIter = condExpr;
            boolean isResult = true;
            while (tempIter != null && isResult) {
                ValueClass valueClass = null;
                ValueClass[] valueClasses = new ValueClass[2];
                ValueClass tupleValueClass = null;
                if (attrType.getAttrType() == AttrType.attrInteger) {
                    if (tempIter.op.attrOperator == AttrOperator.opRANGE) {
                        valueClasses[0] = new IntegerValue(tempIter.operand2.integerRange[0]);
                        valueClasses[1] = new IntegerValue(tempIter.operand2.integerRange[1]);
                    } else {
                        valueClass = new IntegerValue(tempIter.operand2.integer);
                    }
                    tupleValueClass = new IntegerValue(Convert.getIntValue(0, tuple.getTupleByteArray()));
                } else {
                    if (tempIter.op.attrOperator == AttrOperator.opRANGE) {
                        valueClasses[0] = new StringValue(tempIter.operand2.stringRange[0]);
                        valueClasses[1] = new StringValue(tempIter.operand2.stringRange[1]);
                    } else {
                        valueClass = new StringValue(tempIter.operand2.string);
                    }
                    tupleValueClass = new StringValue(Convert.getStringValue(0
                            , tuple.getTupleByteArray(), attrType.getSize()));
                }
                switch (tempIter.op.attrOperator) {
                    case AttrOperator.aopEQ:
                        if (tupleValueClass.compare(valueClass) != 0) isResult = false;
                        break;
                    case AttrOperator.aopGE:
                        if (tupleValueClass.compare(valueClass) < 0) isResult = false;
                        break;
                    case AttrOperator.aopGT:
                        if (tupleValueClass.compare(valueClass) <= 0) isResult = false;
                        break;
                    case AttrOperator.aopLE:
                        if (tupleValueClass.compare(valueClass) > 0) isResult = false;
                        break;
                    case AttrOperator.aopLT:
                        if (tupleValueClass.compare(valueClass) >= 0) isResult = false;
                        break;
                    case AttrOperator.aopNE:
                        if (tupleValueClass.compare(valueClass) == 0) isResult = false;
                        break;
                    case AttrOperator.aopNOP:
                        break;
                    case AttrOperator.aopNOT:
                        break;
                    case AttrOperator.opRANGE:
                        if (tupleValueClass.compare(valueClasses[0]) < 0 || tupleValueClass.compare(valueClasses[1]) > 0)
                            isResult = false;
                        break;
                }
                tempIter = tempIter.next;
            }
            if (isResult) {
                return position;
            }
            tuple = scan.getNext(rid);
        }
        return -1;
    }

    public ArrayList<BitMapFile> makeBitMapFiles() throws Exception {
        String fileName = System.currentTimeMillis() + "";
        BitMapFile bitMapFile = new BitMapFile(fileName, false);
        int count = (int) columnarFile.getTupleCount();
        bitMapFile.Delete(count - 1);
        int getNextIter = getNextPosition();
        while (getNextIter != -1) {
            bitMapFile.Insert(getNextIter);
            getNextIter = getNextPosition();
        }
        bitMapFilesToDelete.add(bitMapFile);
        ArrayList<BitMapFile> bitMapFiles = new ArrayList<BitMapFile>();
        bitMapFiles.add(bitMapFile);
        scan.closeScan();
        return bitMapFiles;
    }


    @Override
    public void destroyEveryThing() throws Exception {
        for (BitMapFile bitMapFile : bitMapFilesToDelete) {
            bitMapFile.destroyBitMapFile();
        }
    }
}
