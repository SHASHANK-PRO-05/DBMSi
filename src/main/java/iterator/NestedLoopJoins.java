package iterator;

import columnar.ColumnarFile;
import columnar.TupleScan;
import global.AttrType;
import global.RID;
import global.TID;
import heap.Tuple;

public class NestedLoopJoins extends Iterator {

    private AttrType[] in1, in2;
    private short[] tuple1StringSizes, tuple2StringSizes;
    private int amountOfMemory;
    private Iterator outerIterator;
    private ColumnarFile columnarFile;
    private CondExpr[] outFiler, rightFilter;
    private FldSpec[] projectionList;
    private int nOutFields;

    private Tuple innerTuple, outerTuple, joinedTuple;
    private boolean done, getFromOuter;
    private TupleScan innerTupleScan;

    /**
     * @param in1
     * @param tuple1StringSizes
     * @param in2
     * @param tuple2StringSizes
     * @param amountOfMemory
     * @param outerIterator
     * @param columnarFileName
     * @param outFilter
     * @param rightFilter
     * @param projectionList
     * @param nOutFields
     * @throws NestedLoopException
     */
    public NestedLoopJoins(
        AttrType[] in1,
        short[] tuple1StringSizes,
        AttrType[] in2,
        short[] tuple2StringSizes,
        int amountOfMemory,
        Iterator outerIterator,
        String columnarFileName,
        CondExpr[] outFilter,
        CondExpr[] rightFilter,
        FldSpec[] projectionList,
        int nOutFields
    ) throws NestedLoopException {

        this.in1 = new AttrType[in1.length];
        System.arraycopy(in1, 0, this.in1, 0, in1.length);
        this.in2 = new AttrType[in2.length];
        System.arraycopy(in2, 0, this.in2, 0, in2.length);

        this.tuple1StringSizes = tuple1StringSizes;
        this.tuple2StringSizes = tuple2StringSizes;
        this.amountOfMemory = amountOfMemory;
        this.outerIterator = outerIterator;

        try {
            this.columnarFile = new ColumnarFile(columnarFileName);
        } catch (Exception e) {
            throw new NestedLoopException("Cannot open columnar file " + columnarFileName);
        }

        this.innerTuple = new Tuple();
        this.outerTuple = new Tuple();
        this.joinedTuple = new Tuple();

        this.outFiler = outFilter;
        this.rightFilter = rightFilter;
        this.projectionList = projectionList;
        this.nOutFields = nOutFields;
    }

    @Override
    public Tuple getNext() throws Exception {
        if (done) {
            return null;
        }

        do {
            if (getFromOuter) {
                getFromOuter = false;

                if (innerTupleScan != null) {
                    innerTupleScan.closeTupleScan();
                    innerTupleScan = null;
                }

                innerTupleScan = new TupleScan(this.columnarFile);

                outerTuple = outerIterator.getNext();

                if (outerTuple == null) {
                    done = true;

                    if (innerTupleScan != null) {
                        innerTupleScan.closeTupleScan();
                        innerTupleScan = null;
                    }

                    return null;
                }
            }

            int numOfRIDs = columnarFile.getColumnarHeader().getColumnCount();
            RID[] rids = new RID[numOfRIDs];

            for (int i = 0; i < numOfRIDs; i++) {
                rids[i] = new RID();
            }

            TID tid = new TID(numOfRIDs, 0, rids);

            innerTuple = innerTupleScan.getNext(tid);

            while (innerTuple != null) {
                innerTuple.setHdr((short) numOfRIDs, in2, tuple2StringSizes);

                if (PredEval.Eval(rightFilter, innerTuple, null, in2, null)) {
                    if (PredEval.Eval(outFiler, outerTuple, innerTuple, in1, in2)) {
                        Projection.Join(outerTuple, in1, innerTuple, in2, joinedTuple, projectionList, nOutFields);

                        return joinedTuple;
                    }
                }
            }

            getFromOuter = true;

        } while (true);
    }

    @Override
    public int getNextPosition() throws Exception {
        return 0;
    }

    @Override
    public void close() {
        try {
            outerIterator.close();
        } catch (Exception e) {
            // TODO: Log Exception
        }
    }
}
