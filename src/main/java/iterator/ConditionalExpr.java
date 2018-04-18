package iterator;

import global.AttrOperator;
import global.AttrType;
import global.IntegerValue;
import global.StringValue;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;

/**
 * ConditionalExpr Evaluator.
 */
public class ConditionalExpr {

    /**
     * @param tuple         Tuple from a columnar file
     * @param colAttributes column details contained inside the tuple
     * @param condExprs     conditional expressions that are in the form of CNF.
     *                      All conditional expressions in the linked list are disjunctions.
     * @return whether the tuple passes all the conditional expressions or not
     */
    static boolean evaluate(Tuple tuple, AttrType[] colAttributes, CondExpr[] condExprs)
        throws IOException, InvalidTupleSizeException, InvalidTypeException, InvalidRelation, FieldNumberOutOfBoundException {

//        ByteToTuple byteToTuple = new ByteToTuple(colAttributes);
//        ArrayList<byte[]> tupleData = byteToTuple.setTupleBytes(tuple.getTupleByteArray());

        for (CondExpr condExpr : condExprs) {
            boolean disjunctionResult = evaluateDisjunctions(tuple, colAttributes, condExpr);

            if (!disjunctionResult) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param tuple         Tuple converted to an arrayList of byte[] based on columns
     * @param colAttributes columns in a tuple
     * @param condExprs     array of conditional expressions
     * @return whether the tuples passes all the disjunctive conditional expressions or not
     */
    private static boolean evaluateDisjunctions(Tuple tuple, AttrType[] colAttributes, CondExpr condExprs)
        throws InvalidTupleSizeException, IOException, InvalidTypeException, InvalidRelation, FieldNumberOutOfBoundException {

        CondExpr currentCondExpr = condExprs;

        while (currentCondExpr != null) {
            int columnNo = currentCondExpr.operand1.symbol.offset;

            if (columnNo >= colAttributes.length) {
                throw new InvalidRelation("Invalid column no when evaluating disjunction");
            }

            AttrType columnAttrType = colAttributes[columnNo - 1];

            int comparisonResult = 0;

            switch (columnAttrType.getAttrType()) {
                case AttrType.attrString:

                    String stringData = tuple.getStrFld(columnNo);

                    StringValue colStringValue = new StringValue(stringData);

                    comparisonResult = colStringValue.compare(new StringValue(currentCondExpr.operand2.string));
                    break;

                case AttrType.attrInteger:

                    int intData = tuple.getIntFld(columnNo);

                    IntegerValue colIntValue = new IntegerValue(intData);

                    comparisonResult = colIntValue.compare(new IntegerValue(currentCondExpr.operand2.integer));
                    break;
            }

            switch (currentCondExpr.op.attrOperator) {
                case AttrOperator.aopEQ:
                    if (comparisonResult == 0) return true;
                    break;
                case AttrOperator.aopNE:
                    if (comparisonResult != 0) return true;
                    break;
                case AttrOperator.aopGE:
                    if (comparisonResult >= 0) return true;
                    break;
                case AttrOperator.aopGT:
                    if (comparisonResult > 0) return true;
                    break;
                case AttrOperator.aopLE:
                    if (comparisonResult <= 0) return true;
                    break;
                case AttrOperator.aopLT:
                    if (comparisonResult < 0) return true;
                    break;
            }


            currentCondExpr = currentCondExpr.next;
        }

        return false;
    }
}
