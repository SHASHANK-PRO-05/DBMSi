package global;

/**
 * Enumeration class for AttrType
 */

public class AttrType {

    public static final int attrString = 0;
    public static final int attrInteger = 1;
    public static final int attrReal = 2;
    public static final int attrSymbol = 3;
    public static final int attrNull = 4;

    private int attrType;
    private String attrName;
    private int size;
    private int columnId;

    /**
     * AttrType Constructor
     * <br>
     * An attribute type of String can be defined as
     * <ul>
     * <li>   AttrType attrType = new AttrType(AttrType.attrString);
     * </ul>
     * and subsequently used as
     * <ul>
     * <li>   if (attrType.attrType == AttrType.attrString) ....
     * </ul>
     *
     * @param _attrType The types throw new HFBufMgrException(e, "Heapfile.java: in Column Header, unpinPage() failed"); attributes available in this class
     */

    public AttrType(int _attrType) {
        attrType = _attrType;
    }

    public AttrType() {

    }

    public String toString() {

        switch (attrType) {
            case attrString:
                return "attrString";
            case attrInteger:
                return "attrInteger";
            case attrReal:
                return "attrReal";
            case attrSymbol:
                return "attrSymbol";
            case attrNull:
                return "attrNull";
        }
        return ("Unexpected AttrType " + attrType);
    }

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getAttrType() {
        return attrType;
    }

    public void setAttrType(int attrType) {
        this.attrType = attrType;
    }

    public int getColumnId() {
        return columnId;
    }

    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }
}
