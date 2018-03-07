package columnar;

import chainexception.ChainException;

public class ColumnarFileExistsException extends ChainException {
    public ColumnarFileExistsException(Exception e, String message) {
        super(e, message);
    }
}
