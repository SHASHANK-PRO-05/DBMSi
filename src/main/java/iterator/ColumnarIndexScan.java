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
import java.util.UUID;


public class ColumnarIndexScan extends Iterator {
    String relName;
    ColumnarFile columnarFile;
    private LateMaterializationUtil[] lateMaterializationUtils;
    private AttrType[] fieldSpecfication;
    private AttrType[] attrTypes;
    ArrayList<BitMapFile> bitMapFiles[];
    ArrayList<BitMapFile> filesToDelete = new ArrayList<>();
    BitMapUtils bitMapUtils;
    ByteToTuple byteToTuple;

    public ColumnarIndexScan(String relName, int[] fldName
            , IndexType[] indexTypes, String[] indexNames
            , AttrType[] attrTypes, short[] strSizes, int noInFlds
            , int noOutFlds, FldSpec[] projectionList, CondExpr[] selects)
            throws Exception {
        this.relName = relName;
        this.attrTypes = attrTypes;
        this.columnarFile = new ColumnarFile(relName);
        this.lateMaterializationUtils = new LateMaterializationUtil[attrTypes.length];
        HashMap<Integer, AttrType> attrTypeHashMap = new HashMap<>();
        for (int i = 0; i < attrTypes.length; i++) {
            attrTypeHashMap.put(attrTypes[i].getColumnId(), attrTypes[i]);
        }
        this.fieldSpecfication = new AttrType[projectionList.length];
        for (int i = 0; i < projectionList.length; i++) {
            fieldSpecfication[i] = attrTypeHashMap.get(projectionList[i].offset);
        }
        byteToTuple = new ByteToTuple(fieldSpecfication);
        bitMapFiles = new ArrayList[selects.length];
        for (int i = 0; i < bitMapFiles.length; i++)
            bitMapFiles[i] = new ArrayList<>();
        for (int i = 0; i < attrTypes.length; i++) {
            if (indexTypes[i].indexType == IndexType.ColumnScan) {
                lateMaterializationUtils[i] = new ColumnarScanUtil(relName, attrTypes[i], selects);
                ArrayList<BitMapFile> tempBitMaps[] = lateMaterializationUtils[i].makeBitMapFile();
                for (int j = 0; j < bitMapFiles.length; j++) {
                    bitMapFiles[j].addAll(tempBitMaps[j]);
                }
            } else if (indexTypes[i].indexType == IndexType.BitMapIndex) {
                lateMaterializationUtils[i] = new BitMapScanUtil(relName, attrTypes[i], selects);
                ArrayList<BitMapFile> tempBitMaps[] = lateMaterializationUtils[i].makeBitMapFile();
                for (int j = 0; j < bitMapFiles.length; j++) {
                    bitMapFiles[j].addAll(tempBitMaps[j]);
                }
            }
        }
        for (int i = 0; i < bitMapFiles.length; i++) {
            BitMapUtils bitMapUtils = new BitMapUtils(bitMapFiles[i]);
            int counter = bitMapUtils.getNextOrPosition();
            BitMapFile bitMapFile = new BitMapFile(UUID.randomUUID().toString(), false);
            while (counter != -1) {
                bitMapFile.Insert(counter);
                counter = bitMapUtils.getNextOrPosition();
            }
            bitMapUtils.closeUtils();
            bitMapFiles[i].clear();
            bitMapFiles[i].add(bitMapFile);
            filesToDelete.add(bitMapFile);
        }
        bitMapUtils = new BitMapUtils(filesToDelete);
    }

    public int getNextPosition() throws Exception {
        int nextPos = bitMapUtils.getNextAndPosition();
        while (nextPos != -1 && columnarFile.isTupleDeletedAtPosition(nextPos)) {
            nextPos = bitMapUtils.getNextAndPosition();
        }
        return nextPos;
    }


    public Tuple getNext() throws Exception {
        int nextPos = bitMapUtils.getNextAndPosition();
        while (nextPos != -1 && columnarFile.isTupleDeletedAtPosition(nextPos)) {
            nextPos = bitMapUtils.getNextAndPosition();
        }
        if (nextPos != -1) {
            Tuple[] tuples = new Tuple[fieldSpecfication.length];
            int size = 0;
            for (int i = 0; i < fieldSpecfication.length; i++) {
                Heapfile heapfile = columnarFile.getHeapFileNames()[fieldSpecfication[i].getColumnId()];
                tuples[i] = heapfile.getRecordAtPosition(nextPos);
                if (tuples[i] == null) {
                    return null;
                }
                size = size + fieldSpecfication[i].getSize();
            }
            return byteToTuple.mergeTuples(tuples, size);
        } else {
            return null;
        }
    }


    public void close() throws Exception {
        for (int i = 0; i < lateMaterializationUtils.length; i++) {
            lateMaterializationUtils[i].destroyEveryThing();
        }
        for (BitMapFile bitMapFile : filesToDelete) {
            bitMapFile.destroyBitMapFile();
        }
        bitMapUtils.closeUtils();
    }
}
