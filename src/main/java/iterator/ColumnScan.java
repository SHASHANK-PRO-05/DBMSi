package iterator;

import java.io.IOException;
import java.util.ArrayList;

import bufmgr.PageNotReadException;
import columnar.ByteToTuple;
import columnar.ColumnarFile;
import columnar.TupleScan;
import global.AttrType;
import global.RID;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;

public class ColumnScan extends Iterator {

    private ColumnarFile columnarFile;
    private CondExpr[] condExprs;
    private FldSpec[] projList;
    private AttrType[] attrTypes;
    private short[] stringSizes;
    private int attrLength;
    private int nOutFields;
    private Tuple tuple;
    private Scan[] scan;
    private int tupleSize;
    private int[] columnNosArray;
    ByteToTuple byteToTuple;
    CondExprEval condExprEval;

    public ColumnScan(String fileName, AttrType[] attrTypes
            , short stringSizes[], int attrLength, int nOutFields
            , FldSpec[] projList, CondExpr[] condExprs) throws ColumnarFileScanException {
        this.attrTypes = attrTypes;
        this.projList = projList;
        this.columnNosArray = new int[attrTypes.length];
        this.condExprs = condExprs;
        this.stringSizes = stringSizes;
        this.attrLength = attrLength;
        this.nOutFields = nOutFields;
        this.tuple = new Tuple();
        this.byteToTuple = new ByteToTuple(attrTypes);
        this.condExprEval = new CondExprEval(attrTypes, condExprs);
        for (int i = 0; i < attrTypes.length; i++)
            columnNosArray[i] = attrTypes[i].getColumnId();
        try {
            //tuple.setHdr((short) this.attrLength, this.attrTypes, this.stringSizes);
        } catch (Exception e) {
            throw new ColumnarFileScanException(e, "Tuple set hdr error");
        }
        this.tupleSize = tuple.getLength();
        try {
            columnarFile = new ColumnarFile(fileName);
        } catch (Exception e) {
            throw new ColumnarFileScanException(e, "Not able to create columnar file");
        }
        try {
            scan = new Scan[attrLength];
            for (int i = 0; i < attrLength; i++) {
                scan[i] = new Scan(columnarFile, (short) columnNosArray[i]);
            }


        } catch (Exception e) {
            throw new ColumnarFileScanException(e, "Not able to initiate scan");
        }
    }

    public FldSpec[] getProjList() {
        return projList;
    }

    public Tuple getNext() throws Exception {
        Tuple projectedTuple = null;
        RID rid = new RID();
        Tuple[] tuples = new Tuple[attrLength];
        ArrayList<byte[]> arrayList = new ArrayList<byte[]>();
        for (int i = 0; i < attrLength; i++) {
            tuples[i] = scan[i].getNext(rid);
            arrayList.add(tuples[i].getTupleByteArray());
        }
        Tuple projectTuples[] = new Tuple[projList.length];
        int sizeOfProjectTuple = 0;
        while (arrayList != null) {
            sizeOfProjectTuple = 0;
            if (condExprEval.isValid(arrayList)) {
                for (int i = 0; i < projList.length; i++) {
                    projectTuples[i] = new Tuple(arrayList.get(i), 0, arrayList.get(i).length);
                    sizeOfProjectTuple += arrayList.get(i).length;
                }
                projectedTuple = byteToTuple.mergeTuples(projectTuples, sizeOfProjectTuple);
                break;
            }
            arrayList.clear();
            for (int i = 0; i < attrLength; i++) {
                tuples[i] = scan[i].getNext(rid);
                if (tuples[i] != null)
                    arrayList.add(tuples[i].getTupleByteArray());
                else
                    return null;
            }

        }
        return projectedTuple;

    }


    @Override
    public void close() throws IOException {
        for (Scan s : scan) {
		    s.closeScan();
        }

    }


}
