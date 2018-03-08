package columnar;

import chainexception.ChainException;

public class ColumnarFilePinPageException extends ChainException {
    public ColumnarFilePinPageException(Exception e, String message) {
        super(e, message);
    }
}
