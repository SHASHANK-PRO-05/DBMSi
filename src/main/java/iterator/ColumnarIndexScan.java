package iterator;

import bitmap.BitMapFile;
import columnar.ByteToTuple;
import columnar.ColumnarFile;
import global.AttrType;
import global.IndexType;
import heap.Heapfile;
import heap.Tuple;

import java.util.ArrayList;
import java.util.HashMap;

public class ColumnarIndexScan {

    private ArrayList<BitMapFile> bitMapFiles = new ArrayList<BitMapFile>();
    ColumnarFile columnarFile;
    private AttrType[] fieldSpecification;
    private ByteToTuple byteToTuple;
    private BitMapUtils bitMapUtils;

    public ColumnarIndexScan(String relName, int[] fldName
            , IndexType[] indexTypes, String[] indexName
            , AttrType[] attrTypes, short[] strSizes, int noInFlds
            , int noOutFlds, FldSpec[] projList, CondExpr[] selects
            , boolean indexOnly) throws Exception {
        this.columnarFile = new ColumnarFile(relName);

        HashMap<Integer, AttrType> hashMap = new HashMap<Integer, AttrType>();
        for (int i = 0; i < attrTypes.length; i++) {
            hashMap.put(attrTypes[i].getColumnId(), attrTypes[i]);
        }
        fieldSpecification = new AttrType[projList.length];
        for (int i = 0; i < projList.length; i++) {
            fieldSpecification[i] = hashMap.get(projList[i].offset);
        }

        byteToTuple = new ByteToTuple(fieldSpecification);


        for (int i = 0; i < selects.length; i++) {
            if (indexTypes[i].indexType == IndexType.ColumnScan) {
                ColumnarScanIndexUtil columnarScanIndexUtil = new ColumnarScanIndexUtil(selects[i], attrTypes[i], relName);
                bitMapFiles.addAll(columnarScanIndexUtil.makeBitMapFiles());
            } else if (indexTypes[i].indexType == IndexType.BitMapIndex) {
                ColumnarScanBitMapIndexUtil columnarScanBitMapIndexUtil = new ColumnarScanBitMapIndexUtil(selects[i]
                        , attrTypes[i], relName);
                bitMapFiles.addAll(columnarScanBitMapIndexUtil.makeBitMapFiles());
            }
        }

        bitMapUtils = new BitMapUtils(bitMapFiles);
    }

    public Tuple getNext() throws Exception {
        int nextPos = bitMapUtils.getNextAndPosition();
        while (nextPos != -1 && columnarFile.isTupleDeletedAtPosition(nextPos)) {
            nextPos = bitMapUtils.getNextAndPosition();
        }
        if (nextPos != -1) {
            Tuple[] tuples = new Tuple[fieldSpecification.length];
            int size = 0;

            for (int i = 0; i < fieldSpecification.length; i++) {
                Heapfile heapfile = columnarFile.getHeapFileNames()[fieldSpecification[i].getColumnId()];
                tuples[i] = heapfile.getRecordAtPosition(nextPos);
                if (tuples[i] == null) {
                    return null;
                }

                size = size + fieldSpecification[i].getSize();
            }
            return byteToTuple.mergeTuples(tuples, size);
        } else {
            return null;
        }
    }


}
