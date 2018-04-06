package iterator;

import global.AttrType;

public class ColumnarScanBtreeIndexUtil extends LateMaterializationUtil {
    private String relName;
    private AttrType attrType;
    private CondExpr condExpr;

    public ColumnarScanBtreeIndexUtil(CondExpr condExpr, AttrType attrType, String relName) {
        this.relName = relName;
        this.attrType = attrType;
        this.condExpr = condExpr;
    }

    @Override
    void destroyEveryThing() {

    }
}
