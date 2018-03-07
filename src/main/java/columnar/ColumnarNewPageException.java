package columnar;

import chainexception.ChainException;

public class ColumnarNewPageException extends ChainException {
    public ColumnarNewPageException(Exception e, String message) {
        super(e, message);
    }
}
