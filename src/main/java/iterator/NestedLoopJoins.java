package iterator;

import columnar.ColumnarFile;
import columnar.TupleScan;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import heap.Tuple;

public class NestedLoopJoins extends Iterator {

    private AttrType[] outerAttributes, innerAttributes;
    private short[] tuple1StringSizes, tuple2StringSizes;
    private int amountOfMemory;
    private Iterator outerIterator;
    private String columnarFileName;
    private ColumnarFile columnarFile;
    private CondExpr[] joinFilter, innerFilter;
    private FldSpec[] projectionList;
    private int nOutFields;
    private IndexType[] indexTypes;
    private FldSpec[] innerProjectionList;

    private Tuple innerTuple, outerTuple, joinedTuple;
    private boolean done, getFromOuter;
    private Iterator innerScan;

    /**
     * @param outerAttributes
     * @param tuple1StringSizes
     * @param innerAttributes
     * @param tuple2StringSizes
     * @param amountOfMemory
     * @param outerIterator
     * @param innerColumnarFileName
     * @param joinFilter
     * @param innerFilter
     * @param projectionList
     * @param nOutFields
     * @throws NestedLoopException
     */
    public NestedLoopJoins(
        AttrType[] outerAttributes,
        short[] tuple1StringSizes,
        AttrType[] innerAttributes,
        short[] tuple2StringSizes,
        int amountOfMemory,
        Iterator outerIterator,
        String innerColumnarFileName,
        CondExpr[] joinFilter,
        CondExpr[] innerFilter,
        FldSpec[] projectionList,
        int nOutFields,
        IndexType[] indexTypes
    ) throws Exception {

        this.outerAttributes = new AttrType[outerAttributes.length];
        System.arraycopy(outerAttributes, 0, this.outerAttributes, 0, outerAttributes.length);
        this.innerAttributes = new AttrType[innerAttributes.length];
        System.arraycopy(innerAttributes, 0, this.innerAttributes, 0, innerAttributes.length);

        this.tuple1StringSizes = tuple1StringSizes;
        this.tuple2StringSizes = tuple2StringSizes;
        this.amountOfMemory = amountOfMemory;
        this.outerIterator = outerIterator;
        this.indexTypes = indexTypes;
        this.columnarFileName = innerColumnarFileName;

        try {
            this.columnarFile = new ColumnarFile(innerColumnarFileName);
        } catch (Exception e) {
            throw new NestedLoopException("Cannot open columnar file " + innerColumnarFileName);
        }

        AttrType[] innerAttrTypes = this.columnarFile.getColumnarHeader().getColumns();

        innerProjectionList = new FldSpec[innerAttrTypes.length];

        for (int i = 0; i < innerAttrTypes.length; i++) {
            innerProjectionList[i] = new FldSpec(new RelSpec(RelSpec.innerRel), i + 1);
        }

        this.innerTuple = new Tuple();
        this.outerTuple = new Tuple();
        this.joinedTuple = new Tuple();

        this.joinFilter = joinFilter;
        this.innerFilter = innerFilter;
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

                if (innerScan != null) {
                    innerScan.close();
                    innerScan = null;
                }

                outerTuple = outerIterator.getNext();

                CondExpr[] innerJoinFilter = new CondExpr[1];
                innerJoinFilter[0] = new CondExpr();
                innerJoinFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
                innerJoinFilter[0].operand1 = joinFilter[0].operand2;

                int outerJoinColumnID = joinFilter[0].operand1.symbol.offset;
                AttrType outerJoinColumnType = outerAttributes[outerJoinColumnID - 1];

                if (outerJoinColumnType.getAttrType() == AttrType.attrString) {
                    innerJoinFilter[0].operand2.string = outerTuple.getStrFld(outerJoinColumnID);
                } else if (outerJoinColumnType.getAttrType() == AttrType.attrInteger) {
                    innerJoinFilter[0].operand2.integer = outerTuple.getIntFld(outerJoinColumnID);
                }

                if (indexTypes == null || indexTypes.length == 0) {
                    innerScan = new TupleScan(columnarFile);
                    CondExpr[] newInnerFilter = new CondExpr[innerFilter.length + 1];

                    System.arraycopy(innerFilter, 0, newInnerFilter, 0, innerFilter.length);
                    newInnerFilter[innerFilter.length] = innerJoinFilter[0];

                    innerFilter = newInnerFilter;

                } else {
                    innerScan = new ColumnarIndexScan(columnarFileName, null, indexTypes,
                        null, innerAttributes, null, innerAttributes.length, innerAttributes.length,
                        innerProjectionList, innerJoinFilter);
                }

                if (outerTuple == null) {
                    done = true;

                    if (innerScan != null) {
                        innerScan.close();
                        innerScan = null;
                    }

                    return null;
                }
            }

            innerTuple = innerScan.getNext();

            while (innerTuple != null) {
                if (ConditionalExpr.evaluate(innerTuple, innerAttributes, innerFilter)) {
                    Projection.Join(outerTuple, outerAttributes, innerTuple, innerAttributes, joinedTuple, projectionList, nOutFields);

                    return joinedTuple;
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
