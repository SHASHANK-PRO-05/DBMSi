package columnar;

import chainexception.ChainException;

public class ColumnarMetaInsertException extends ChainException {
    public ColumnarMetaInsertException(Exception e, String message) {
        super(e, message);
    }
}
