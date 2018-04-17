package iterator;

import bitmap.BitMapFile;
import columnar.ColumnarFile;
import global.*;
import heap.Scan;
import heap.Tuple;

import java.util.ArrayList;
import java.util.UUID;

public class ColumnarScanUtil implements LateMaterializationUtil {
    private String relName;
    private ColumnarFile columnarFile;
    private CondExpr[] condExprs;
    AttrType attrType;
    ArrayList<BitMapFile> bitMapFiles[];
    BitMapLinkedList[] bitMapLinkedLists;
    ArrayList<BitMapFile> filesToDelete = new ArrayList<>();

    public ColumnarScanUtil(String relName, AttrType attrType, CondExpr[] condExprs) throws Exception {
        columnarFile = new ColumnarFile(relName);
        this.relName = relName;
        this.condExprs = condExprs;
        this.attrType = attrType;
        bitMapLinkedLists = new BitMapLinkedList[condExprs.length];
        for (int i = 0; i < condExprs.length; i++) {
            CondExpr iter = condExprs[i];
            bitMapLinkedLists[i] = new BitMapLinkedList();
            bitMapLinkedLists[i].bitMapFile = new BitMapFile(UUID.randomUUID().toString(), false);
            iter = iter.next;
            BitMapLinkedList bitMapIter = bitMapLinkedLists[i];
            filesToDelete.add(bitMapIter.bitMapFile);
            while (iter != null) {
                bitMapIter.next = new BitMapLinkedList();
                bitMapIter = bitMapIter.next;
                bitMapIter.bitMapFile = new BitMapFile(UUID.randomUUID().toString(), false);
                filesToDelete.add(bitMapIter.bitMapFile);
                iter = iter.next;
            }
        }
    }

    @Override
    public ArrayList<BitMapFile>[] makeBitMapFile() throws Exception {
        Scan scan = new Scan(columnarFile, (short) attrType.getColumnId());
        RID rid = new RID();
        Tuple tuple = scan.getNext(rid);
        int counter = 0;
        while (tuple != null) {
            for (int i = 0; i < condExprs.length; i++) {
                CondExpr condExpr = condExprs[i];
                BitMapLinkedList bitMapLinkedList = bitMapLinkedLists[i];
                while (condExpr != null) {
                    if (condExpr.operand1.symbol.offset == attrType.getColumnId()) {
                        ValueClass valueClass;
                        ValueClass compareTo;
                        if (attrType.getAttrType() == AttrType.attrInteger) {
                            valueClass = new IntegerValue(Convert.getIntValue(0, tuple.getTupleByteArray()));
                            compareTo = new IntegerValue(condExpr.operand2.integer);
                        } else {
                            valueClass = new StringValue(Convert.getStringValue(0, tuple.getTupleByteArray(), attrType.getSize()));
                            compareTo = new StringValue(condExpr.operand2.string);
                        }
                        switch (condExpr.op.attrOperator) {
                            case AttrOperator.aopEQ:
                                if (valueClass.compare(compareTo) == 0)
                                    bitMapLinkedList.bitMapFile.Insert(counter);
                                else
                                    bitMapLinkedList.bitMapFile.Delete(counter);
                                break;
                            case AttrOperator.aopGE:
                                if (valueClass.compare(compareTo) >= 0)
                                    bitMapLinkedList.bitMapFile.Insert(counter);
                                else
                                    bitMapLinkedList.bitMapFile.Delete(counter);
                                break;
                            case AttrOperator.aopGT:
                                if (valueClass.compare(compareTo) > 0)
                                    bitMapLinkedList.bitMapFile.Insert(counter);
                                else
                                    bitMapLinkedList.bitMapFile.Delete(counter);
                                break;
                            case AttrOperator.aopLE:
                                if (valueClass.compare(compareTo) <= 0)
                                    bitMapLinkedList.bitMapFile.Insert(counter);
                                else
                                    bitMapLinkedList.bitMapFile.Delete(counter);
                                break;
                            case AttrOperator.aopLT:
                                if (valueClass.compare(compareTo) < 0)
                                    bitMapLinkedList.bitMapFile.Insert(counter);
                                else
                                    bitMapLinkedList.bitMapFile.Delete(counter);
                                break;
                            case AttrOperator.aopNE:
                                if (valueClass.compare(compareTo) != 0)
                                    bitMapLinkedList.bitMapFile.Insert(counter);
                                else
                                    bitMapLinkedList.bitMapFile.Delete(counter);
                                break;
                        }

                    } else {
                        bitMapLinkedList.bitMapFile.Delete(counter);
                    }
                    condExpr = condExpr.next;
                    bitMapLinkedList = bitMapLinkedList.next;
                }
            }
            counter++;
            tuple = scan.getNext(rid);
        }
        scan.closeScan();
        bitMapFiles = new ArrayList[condExprs.length];
        for (int i = 0; i < bitMapFiles.length; i++) {
            bitMapFiles[i] = new ArrayList<>();
            BitMapLinkedList bitMapLinkedList = bitMapLinkedLists[i];
            while (bitMapLinkedList != null) {
                bitMapFiles[i].add(bitMapLinkedList.bitMapFile);
                bitMapLinkedList = bitMapLinkedList.next;
            }
        }
        return bitMapFiles;
    }

    public void destroyEveryThing() throws Exception {
        for (BitMapFile bitMapFile : filesToDelete) {
            bitMapFile.destroyBitMapFile();
        }
    }
}
