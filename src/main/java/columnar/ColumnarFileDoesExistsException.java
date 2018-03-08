package columnar;

import chainexception.ChainException;

public class ColumnarFileDoesExistsException extends ChainException {
    public ColumnarFileDoesExistsException(Exception e, String message) {
        super(e, message);
    }
}
