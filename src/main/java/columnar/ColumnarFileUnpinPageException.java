package columnar;

import chainexception.ChainException;

public class ColumnarFileUnpinPageException extends ChainException {
    public ColumnarFileUnpinPageException(Exception e, String message) {
        super(e, message);
    }
}
