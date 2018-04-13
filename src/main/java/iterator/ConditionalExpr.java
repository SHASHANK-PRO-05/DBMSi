package iterator;

import columnar.ByteToTuple;
import global.*;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;
import java.util.ArrayList;

/**
 * ConditionalExpr Evaluator.
 */
public class ConditionalExpr {

    /**
     * @param tuple         Tuple from a columnar file
     * @param colAttributes column details contained inside the tuple
     * @param condExprs     conditional expressions that are in the form of CNF.
     *                      All conditional expressions in a single row are disjunctions
     * @return whether the tuple passes all the conditional expressions or not
     */
    static boolean evaluate(Tuple tuple, AttrType[] colAttributes, CondExpr[][] condExprs)
        throws IOException, InvalidTupleSizeException, InvalidTypeException, InvalidRelation {

        ByteToTuple byteToTuple = new ByteToTuple(colAttributes);
        ArrayList<byte[]> tupleData = byteToTuple.setTupleBytes(tuple.getTupleByteArray());

        for (CondExpr[] condExpr : condExprs) {
            boolean disjunctionResult = evaluateDisjunctions(tupleData, colAttributes, condExpr);

            if (!disjunctionResult) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param tupleData     Tuple converted to an arrayList of byte[] based on columns
     * @param colAttributes columns in a tuple
     * @param condExprs     array of conditional expressions
     * @return whether the tuples passes all the disjunctive conditional expressions or not
     */
    private static boolean evaluateDisjunctions(ArrayList<byte[]> tupleData, AttrType[] colAttributes, CondExpr[] condExprs)
        throws InvalidTupleSizeException, IOException, InvalidTypeException, InvalidRelation {

        if (tupleData.size() != colAttributes.length) {
            throw new InvalidTupleSizeException();
        }

        for (int i = 0; i < condExprs.length; i++) {
            CondExpr condExpr = condExprs[i];

            int columnNo = condExpr.operand1.symbol.offset;

            if (columnNo >= colAttributes.length) {
                throw new InvalidRelation("Invalid column no when evaluating disjunction");
            }

            AttrType columnAttrType = colAttributes[columnNo];

            int comparisonResult = 0;
            byte[] colData = null;

            switch (columnAttrType.getAttrType()) {
                case AttrType.attrString:

                    colData = tupleData.get(i);

                    StringValue colStringValue = new StringValue(
                        Convert.getStringValue(0, colData, columnAttrType.getSize())
                    );

                    comparisonResult = colStringValue.compare(new StringValue(condExpr.operand2.string));
                    break;

                case AttrType.attrInteger:

                    colData = tupleData.get(i);

                    IntegerValue colIntValue = new IntegerValue(Convert.getIntValue(0, colData));

                    comparisonResult = colIntValue.compare(new IntegerValue(condExpr.operand2.integer));
                    break;
            }

            if (colData == null) {
                throw new InvalidTypeException();
            }

            switch (condExpr.op.attrOperator) {
                case AttrOperator.aopEQ:
                    if (comparisonResult == 0) return true;
                case AttrOperator.aopNE:
                    if (comparisonResult != 0) return true;
                case AttrOperator.aopGE:
                    if (comparisonResult >= 0) return true;
                case AttrOperator.aopGT:
                    if (comparisonResult > 0) return true;
                case AttrOperator.aopLE:
                    if (comparisonResult <= 0) return true;
                case AttrOperator.aopLT:
                    if (comparisonResult < 0) return true;
            }
        }

        return false;
    }
}
