package iterator;

import bitmap.BitMapFile;
import bitmap.BitMapScanException;
import columnar.ByteToTuple;
import columnar.ColumnarFile;
import columnar.IndexInfo;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.StringValue;
import heap.HFBufMgrException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class BitmapScan extends Iterator {
    private BitMapUtils bitMapUtils;
    private ColumnarFile columnarFile = null;
    private AttrType[] attrTypes;
    private AttrType[] fieldSpecification;
    private ByteToTuple byteToTuple;

    public BitmapScan(String fileName, AttrType[] attrTypes
            , short stringSizes[], int attrLength, int nOutFields
            , FldSpec[] projList, CondExpr[] condExprs)
            throws BitMapScanException
            , HFBufMgrException, IOException
            , InvalidSlotNumberException, Exception {
        this.attrTypes = attrTypes;
        try {
            columnarFile = new ColumnarFile(fileName);
        } catch (Exception e) {
            throw new BitMapScanException(null, "not able to open columnar file");
        }
        HashMap<Integer, AttrType> hashMap = new HashMap<Integer, AttrType>();
        for (int i = 0; i < attrTypes.length; i++) {
            hashMap.put(attrTypes[i].getColumnId(), attrTypes[i]);
        }
        fieldSpecification = new AttrType[projList.length];
        for (int i = 0; i < projList.length; i++) {
            fieldSpecification[i] = hashMap.get(projList[i].offset);
        }

        byteToTuple = new ByteToTuple(fieldSpecification);


        ArrayList<BitMapFile> bitMapFiles = new ArrayList<BitMapFile>();

        for (int i = 0; i < condExprs.length - 1; i++) {
            ArrayList<IndexInfo> indexInfos = columnarFile.getColumnarHeader()
                    .getParticularTypeIndex(condExprs[i].operand1.symbol.offset, new IndexType(3));
            for (IndexInfo indexInfo : indexInfos) {
                if (indexInfo.getValue() instanceof StringValue) {
                    switch (condExprs[i].op.attrOperator) {
                        case AttrOperator.aopLE:
                            if (indexInfo.getValue().getValue().toString()
                                    .compareTo(condExprs[i].operand2.string) <= 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                        case AttrOperator.aopLT:
                            if (indexInfo.getValue().getValue().toString()
                                    .compareTo(condExprs[i].operand2.string) < 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                        case AttrOperator.aopEQ:
                            if (indexInfo.getValue().getValue().toString()
                                    .compareTo(condExprs[i].operand2.string) == 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                        case AttrOperator.aopGE:
                            if (indexInfo.getValue().getValue().toString()
                                    .compareTo(condExprs[i].operand2.string) >= 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                        case AttrOperator.aopGT:
                            if (indexInfo.getValue().getValue().toString()
                                    .compareTo(condExprs[i].operand2.string) > 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                        case AttrOperator.aopNE:
                            if (indexInfo.getValue().getValue().toString()
                                    .compareTo(condExprs[i].operand2.string) != 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                    }
                } else {
                    switch (condExprs[i].op.attrOperator) {
                        case AttrOperator.aopLE:
                            if (Integer.parseInt(indexInfo.getValue().getValue().toString()) -
                                    condExprs[i].operand2.integer <= 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                        case AttrOperator.aopLT:
                            if (Integer.parseInt(indexInfo.getValue().getValue().toString()) -
                                    condExprs[i].operand2.integer < 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                        case AttrOperator.aopEQ:
                            if (Integer.parseInt(indexInfo.getValue().getValue().toString()) -
                                    condExprs[i].operand2.integer == 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                        case AttrOperator.aopGE:
                            if (Integer.parseInt(indexInfo.getValue().getValue().toString()) -
                                    condExprs[i].operand2.integer >= 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                        case AttrOperator.aopGT:
                            if (Integer.parseInt(indexInfo.getValue().getValue().toString()) -
                                    condExprs[i].operand2.integer > 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                        case AttrOperator.aopNE:
                            if (Integer.parseInt(indexInfo.getValue().getValue().toString()) -
                                    condExprs[i].operand2.integer != 0)
                                bitMapFiles.add(new BitMapFile(indexInfo.getFileName()));
                            break;
                    }
                }
            }
        }
        //System.out.println(bitMapFiles.size());
        bitMapUtils = new BitMapUtils(bitMapFiles);
    }

    public Tuple getNext() throws Exception {
        int ans = bitMapUtils.getNextOrPosition();
        if (ans != -1) {

            Tuple[] tuples = new Tuple[fieldSpecification.length];
            int size = 0;
            for (int i = 0; i < fieldSpecification.length; i++) {
                Heapfile heapfile = columnarFile.getHeapFileNames()[fieldSpecification[i].getColumnId()];
                tuples[i] = heapfile.getRecordAtPosition(ans);
                size = size + fieldSpecification[i].getSize();
            }
            return byteToTuple.mergeTuples(tuples, size);
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            bitMapUtils.closeUtils();
        } catch (Exception e) {
            System.out.println(e);
        }

    }
}