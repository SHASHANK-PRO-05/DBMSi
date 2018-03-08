package columnar;

import global.IndexType;
import global.ValueClass;

public class IndexInfo {

    private int columnNumber;
    private ValueClass value;
    private String fileName;
    private IndexType indextype;


    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public IndexType getIndextype() {
        return indextype;
    }

    public void setIndexType(IndexType indextype) {
        this.indextype = indextype;
    }

    public int getColumnNumber() {
        return columnNumber;
    }

    public void setColumnNumber(int columnNumber) {
        this.columnNumber = columnNumber;
    }

    public ValueClass getValue() {
        return value;
    }

    public void setValue(ValueClass value) {
        this.value = value;
    }


}