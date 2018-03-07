package columnar;

import chainexception.ChainException;

public class ColumnHeaderCreateException extends ChainException {
    public ColumnHeaderCreateException() {
        super();

    }

    public ColumnHeaderCreateException(Exception ex, String name) {
        super(ex, name);
    }
}
