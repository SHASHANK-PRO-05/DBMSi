package bitmap;

import chainexception.ChainException;

public class BitMapScanException extends ChainException {
    public BitMapScanException(Exception e, String message) {
        super(e, message);
    }
}
