package iterator;

import bitmap.BitMapFile;
import btree.*;
import columnar.ColumnarFile;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.TID;

import java.util.ArrayList;
import java.util.UUID;

public class BtreeScanUtil implements LateMaterializationUtil {
    String relName;
    AttrType attrType;
    CondExpr[] condExprs;
    ColumnarFile columnarFile;
    KeyClass lowKey;
    KeyClass highKey;
    BTreeFile bTreeFile;
    IndexFileScan indexFileScan;
    BitMapLinkedList[] bitMapLinkedLists;
    ArrayList<BitMapFile> filesToDelete = new ArrayList<>();
    ArrayList<BitMapFile> bitMapFiles[];

    public BtreeScanUtil(String relName,
                         AttrType attrType, CondExpr[] condExprs) throws Exception {
        columnarFile = new ColumnarFile(relName);
        this.relName = relName;
        this.attrType = attrType;
        this.condExprs = condExprs;

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

        int lowInt = 0;
        int highInt = 0;
        String lowString = "";
        String highString = "";
        bTreeFile = new BTreeFile(columnarFile.getColumnarHeader().getParticularTypeIndex(attrType.getColumnId()
                , new IndexType(IndexType.B_Index)).get(0).getFileName());

        if (attrType.getAttrType() == AttrType.attrInteger) {
            lowInt = Integer.MAX_VALUE;
            highInt = Integer.MIN_VALUE;
        } else {
            lowString = highestStringValue();
            highString = smallestStringValue();
        }
        for (int i = 0; i < condExprs.length; i++) {
            CondExpr condExpr = condExprs[i];
            while (condExpr != null) {
                if (condExpr.operand1.symbol.offset == attrType.getColumnId()) {
                    KeyClass keyClass;
                    if (attrType.getAttrType() == AttrType.attrInteger) {
                        keyClass = new IntegerKey(condExpr.operand2.integer);
                        switch (condExpr.op.attrOperator) {
                            case AttrOperator.aopEQ:
                                lowInt = Math.min(lowInt, condExpr.operand2.integer);
                                highInt = Math.max(highInt, condExpr.operand2.integer);
                                break;
                            case AttrOperator.aopGT:
                            case AttrOperator.aopGE:
                                lowInt = Math.min(condExpr.operand2.integer, lowInt);
                                highInt = Integer.MAX_VALUE;
                                break;
                            case AttrOperator.aopLE:
                            case AttrOperator.aopLT:
                                lowInt = Integer.MIN_VALUE;
                                highInt = Math.max(highInt, condExpr.operand2.integer);
                                break;
                            case AttrOperator.aopNE:
                                lowInt = Integer.MIN_VALUE;
                                highInt = Integer.MAX_VALUE;
                                break;
                        }
                    } else {
                        keyClass = new StringKey(condExpr.operand2.string);
                        switch (condExpr.op.attrOperator) {
                            case AttrOperator.aopEQ:
                                lowString = condExpr.operand2.string.compareTo(lowString) > 0 ? lowString : condExpr.operand2.string;
                                highString = condExpr.operand2.string.compareTo(highString) > 0 ? condExpr.operand2.string : highString;
                                break;
                            case AttrOperator.aopGT:
                            case AttrOperator.aopGE:
                                lowString = condExpr.operand2.string.compareTo(lowString) > 0 ? lowString : condExpr.operand2.string;
                                highString = highestStringValue();
                                break;
                            case AttrOperator.aopLE:
                            case AttrOperator.aopLT:
                                lowString = smallestStringValue();
                                highString = condExpr.operand2.string.compareTo(highString) > 0 ? condExpr.operand2.string : highString;
                                break;
                            case AttrOperator.aopNE:
                                lowString = smallestStringValue();
                                highString = highestStringValue();
                                break;
                        }
                    }
                }
                condExpr = condExpr.next;
            }
        }
        if (attrType.getAttrType() == AttrType.attrInteger) {
            lowKey = new IntegerKey(lowInt);
            highKey = new IntegerKey(highInt);
        } else {
            lowKey = new StringKey(lowString);
            highKey = new StringKey(highString);
        }
        indexFileScan = bTreeFile.new_scan(lowKey, highKey);
    }

    public String highestStringValue() {
        char ch = Character.MAX_VALUE;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < attrType.getSize(); i++) {
            stringBuilder.append(ch);
        }
        return stringBuilder.toString();
    }

    public String smallestStringValue() {
        char ch = Character.MIN_VALUE;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < attrType.getSize(); i++) {
            stringBuilder.append(ch);
        }
        return stringBuilder.toString();
    }

    @Override
    public ArrayList<BitMapFile>[] makeBitMapFile() throws Exception {
        KeyDataEntry keyDataEntry = indexFileScan.get_next();
        while (keyDataEntry != null) {
            KeyClass keyClass = keyDataEntry.key;
            TID tempTid = ((LeafData) keyDataEntry.data).getData();
            int position = tempTid.getPosition();
            for (int i = 0; i < condExprs.length; i++) {
                CondExpr condExpr = condExprs[i];
                BitMapLinkedList bitMapLinkedList = bitMapLinkedLists[i];

                while (condExpr != null) {
                    if (condExpr.operand1.symbol.offset == attrType.getColumnId()) {
                        KeyClass valueClass;
                        if (attrType.getAttrType() == AttrType.attrInteger) {
                            valueClass = new IntegerKey(condExpr.operand2.integer);
                        } else {
                            valueClass = new StringKey(condExpr.operand2.string);
                        }


                        switch (condExpr.op.attrOperator) {
                            case AttrOperator.aopEQ:
                                if (keyClass.compare(valueClass) == 0)
                                    bitMapLinkedList.bitMapFile.Insert(position);
                                break;
                            case AttrOperator.aopGT:
                                if (keyClass.compare(valueClass) > 0)
                                    bitMapLinkedList.bitMapFile.Insert(position);
                                break;
                            case AttrOperator.aopGE:
                                if (keyClass.compare(valueClass) >= 0)
                                    bitMapLinkedList.bitMapFile.Insert(position);
                                break;
                            case AttrOperator.aopLE:
                                if (keyClass.compare(valueClass) <= 0)
                                    bitMapLinkedList.bitMapFile.Insert(position);
                                break;
                            case AttrOperator.aopLT:
                                if (keyClass.compare(valueClass) < 0)
                                    bitMapLinkedList.bitMapFile.Insert(position);
                                break;
                            case AttrOperator.aopNE:
                                if (keyClass.compare(valueClass) != 0)
                                    bitMapLinkedList.bitMapFile.Insert(position);
                                break;
                        }
                    } else {
                        bitMapLinkedList.bitMapFile.Delete(position);
                    }
                    condExpr = condExpr.next;
                    bitMapLinkedList = bitMapLinkedList.next;
                }
            }
            keyDataEntry = indexFileScan.get_next();
        }
        bTreeFile.close();
        ((BTFileScan) indexFileScan).DestroyBTreeFileScan();
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

    @Override
    public void destroyEveryThing() throws Exception {
        for (BitMapFile bitMapFile : filesToDelete) {
            bitMapFile.destroyBitMapFile();
        }
    }
}
