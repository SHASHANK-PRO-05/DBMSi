package iterator;

import global.*;

import java.io.IOException;
import java.util.ArrayList;

public class CondExprEval {
    AttrType[] attrTypes;
    CondExpr[] condExprs;
    ValueClass[] secondOperand;
    int[] relativeColumnIndexs;


    public CondExprEval(AttrType[] attrType, CondExpr[] condExpr) {
        this.attrTypes = attrType;
        this.condExprs = condExpr;
        relativeColumnIndexs = new int[condExpr.length - 1];
        secondOperand = new ValueClass[condExpr.length - 1];

        for (int i = 0; i < this.condExprs.length - 1; i++) {
            int j;
            for (j = 0; j < attrType.length; j++) {
                if (attrType[j].getColumnId() == condExpr[i].operand1.symbol.offset)
                    break;
            }
            relativeColumnIndexs[i] = j;
            if (attrType[j].getAttrType() == AttrType.attrString) {
                secondOperand[i] = new StringValue(condExpr[i]
                        .operand2.string);
            } else if (attrType[j].getAttrType() == AttrType.attrInteger) {
                secondOperand[i] = new IntegerValue(condExpr[i].operand2.integer);
            }
        }


    }

    public boolean isValid(ArrayList<byte[]> arrayList) throws IOException {
        for (int i = 0; i < condExprs.length - 1; i++) {
            AttrType attrType = attrTypes[relativeColumnIndexs[i]];
            int op = 0;
            switch (attrType.getAttrType()) {
                case AttrType.attrString:
                    StringValue stringValue = new StringValue(Convert
                            .getStringValue(0, arrayList.get(relativeColumnIndexs[i]), attrType.getSize()));
                    op = stringValue.compare(secondOperand[i]);
                    break;
                case AttrType.attrInteger:
                    IntegerValue integerValue = new IntegerValue(Convert.getIntValue(0, arrayList.get(relativeColumnIndexs[i])));
                    op = integerValue.compare(secondOperand[i]);
                    break;
            }

            switch (condExprs[i].op.attrOperator) {
                case AttrOperator.aopEQ:
                    if (op == 0) return true;
                    break;
                case AttrOperator.aopLT:
                    if (op < 0) return true;
                    break;
                case AttrOperator.aopGT:
                    if (op > 0) return true;
                    break;
                case AttrOperator.aopNE:
                    if (op != 0) return true;
                    break;
                case AttrOperator.aopLE:
                    if (op <= 0) return true;
                    break;
                case AttrOperator.aopGE:
                    if (op >= 0) return true;
                    break;
                default:
                    break;
            }
        }
        return false;
    }
}
