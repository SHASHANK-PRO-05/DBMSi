package columnar;


import global.AttrType;

public class AttrInfo {

    private int columnId;
    private int attrType;
    private int attrSize;

    private String columnName;

    public int getColumnId() {
        return columnId;
    }

    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }

    public int getAttrType() {
        return attrType;
    }

    public void setAttrType(int attrType) {
        this.attrType = attrType;
    }

    public int getAttrSize() {
        return attrSize;
    }

    public void setAttrSize(int attrSize) {
        this.attrSize = attrSize;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
