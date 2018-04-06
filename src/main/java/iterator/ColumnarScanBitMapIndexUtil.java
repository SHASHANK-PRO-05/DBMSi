package iterator;

import bitmap.BitMapFile;
import columnar.ColumnarFile;
import columnar.IndexInfo;
import global.*;

import java.util.ArrayList;

public class ColumnarScanBitMapIndexUtil {
    ColumnarFile columnarFile;
    ArrayList<IndexInfo> indexInfos;
    AttrType attrType;
    String relName;
    CondExpr condExpr;
    private ArrayList<BitMapFile> bitMapFilesToDelete = new ArrayList<BitMapFile>();

    public ColumnarScanBitMapIndexUtil(CondExpr condExpr, AttrType attrType, String relName)
            throws Exception {
        this.columnarFile = new ColumnarFile(relName);
        this.attrType = attrType;
        this.relName = relName;
        indexInfos = columnarFile.getColumnarHeader().getParticularTypeIndex(attrType.getColumnId()
                , new IndexType(IndexType.BitMapIndex));
        this.condExpr = condExpr;
    }

    public ArrayList<BitMapFile> makeBitMapFiles() throws Exception {
        ArrayList<BitMapFile> bitMapFiles = new ArrayList<BitMapFile>();
        ArrayList<BitMapFile> ored = new ArrayList<BitMapFile>();

        for (IndexInfo indexInfo : indexInfos) {
            CondExpr tempIter = condExpr;
            ValueClass valueClass = null;
            ValueClass[] valueClasses = new ValueClass[2];
            if (attrType.getAttrType() == AttrType.attrInteger) {
                if (tempIter.op.attrOperator == AttrOperator.opRANGE) {
                    valueClasses[0] = new IntegerValue(tempIter.operand2.integerRange[0]);
                    valueClasses[1] = new IntegerValue(tempIter.operand2.integerRange[1]);
                } else {
                    valueClass = new IntegerValue(tempIter.operand2.integer);
                }
            } else {
                if (tempIter.op.attrOperator == AttrOperator.opRANGE) {
                    valueClasses[0] = new StringValue(tempIter.operand2.stringRange[0]);
                    valueClasses[1] = new StringValue(tempIter.operand2.stringRange[1]);
                } else {
                    valueClass = new StringValue(tempIter.operand2.string);
                }
            }
            while (tempIter != null) {
                switch (condExpr.op.attrOperator) {
                    case AttrOperator.aopEQ:
                        if (indexInfo.getValue().compare(valueClass) == 0)
                            ored.add(new BitMapFile(indexInfo.getFileName()));
                        break;
                    case AttrOperator.aopGE:
                        if (indexInfo.getValue().compare(valueClass) >= 0)
                            ored.add(new BitMapFile(indexInfo.getFileName()));
                        break;
                    case AttrOperator.aopGT:
                        if (indexInfo.getValue().compare(valueClass) > 0) {
                            ored.add(new BitMapFile(indexInfo.getFileName()));
                        }
                        break;
                    case AttrOperator.aopLE:
                        if (indexInfo.getValue().compare(valueClass) <= 0) {
                            ored.add(new BitMapFile(indexInfo.getFileName()));
                        }
                        break;
                    case AttrOperator.aopLT:
                        if (indexInfo.getValue().compare(valueClass) < 0) {
                            ored.add(new BitMapFile(indexInfo.getFileName()));
                        }
                        break;
                    case AttrOperator.aopNE:
                        if (indexInfo.getValue().compare(valueClass) != 0) {
                            ored.add(new BitMapFile(indexInfo.getFileName()));
                        }
                        break;
                    case AttrOperator.aopNOP:
                        break;
                    case AttrOperator.aopNOT:
                        break;
                    case AttrOperator.opRANGE:
                        if (indexInfo.getValue().compare(valueClasses[0]) >= 0 && indexInfo.getValue().compare(valueClasses[1]) <= 0) {
                            ored.add(new BitMapFile(indexInfo.getFileName()));
                        }
                        break;
                }
                tempIter = tempIter.next;
            }

        }
        BitMapFile bitMapFile = new BitMapFile(relName
                + "b.t" + attrType.getColumnId(), false);
        bitMapFile.Delete((int) columnarFile.getTupleCount() - 1);
        bitMapFilesToDelete.add(bitMapFile);
        BitMapUtils bitMapUtils = new BitMapUtils(ored);
        int nextPos = bitMapUtils.getNextOrPosition();
        while (nextPos != -1) {
            bitMapFile.Insert(nextPos);
            nextPos = bitMapUtils.getNextOrPosition();
        }
        bitMapFiles.add(bitMapFile);
        return bitMapFiles;
    }


}
