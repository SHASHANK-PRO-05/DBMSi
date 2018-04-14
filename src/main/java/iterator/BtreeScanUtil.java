package iterator;

import bitmap.BitMapFile;
import btree.IntegerKey;
import btree.KeyClass;
import btree.StringKey;
import columnar.ColumnarFile;
import global.AttrOperator;
import global.AttrType;

import java.util.ArrayList;

public class BtreeScanUtil implements LateMaterializationUtil {
    String relName;
    AttrType attrType;
    CondExpr[] condExprs;
    ColumnarFile columnarFile;
    KeyClass lowKey;
    KeyClass highKey;

    public BtreeScanUtil(String relName,
                         AttrType attrType, CondExpr[] condExprs) throws Exception {
        columnarFile = new ColumnarFile(relName);
        this.relName = relName;
        this.attrType = attrType;
        this.condExprs = condExprs;
        if (attrType.getAttrType() == AttrType.attrInteger) {
            lowKey = new IntegerKey(Integer.MAX_VALUE);
            highKey = new IntegerKey(Integer.MIN_VALUE);
        } else {
            lowKey = new StringKey("");
            highKey = new StringKey("");
        }
        for (int i = 0; i < condExprs.length; i++) {
            CondExpr condExpr = condExprs[i];
            while (condExpr != null) {
                if (condExpr.operand1.symbol.offset == attrType.getColumnId()) {
                    KeyClass keyClass;
                    if (attrType.getAttrType() == AttrType.attrInteger) {
                        keyClass = new IntegerKey(condExpr.operand2.integer);
                    } else {
                        keyClass = new StringKey(condExpr.operand2.string);
                    }
                    switch (condExpr.op.attrOperator) {
                        case AttrOperator.aopEQ:
                            break;
                        case AttrOperator.aopGT:
                        case AttrOperator.aopGE:
                            if (attrType.getAttrType() == AttrType.attrInteger) {
                                lowKey = new IntegerKey(Math.min(((IntegerKey) keyClass).getKey(), ((IntegerKey) lowKey).getKey()));
                                highKey = null;
                            } else {
                                highKey = null;
                                if (((StringKey) lowKey).getKey().compareTo(condExpr.operand2.string) > 0) {
                                    lowKey = keyClass;
                                }
                            }
                            break;
                    }
                }
                condExpr = condExpr.next;
            }
        }
    }

    @Override
    public ArrayList<BitMapFile>[] makeBitMapFile() {

        return null;
    }

    @Override
    public void destroyEveryThing() {

    }
}
