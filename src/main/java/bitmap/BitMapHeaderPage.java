package bitmap;
import heap.*;

//The commented code is because of dependency on BMPage

public class BitMapHeaderPage extends HFPage{

    private int columnIndex;
    /*
    private BMPage startPage;

    public BitMapHeaderPage() {
    }

    public BitMapHeaderPage(int columnIndex, BMPage startPage) {
        this.columnIndex = columnIndex;
        this.startPage = startPage;
    }*/


    public int getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    /*
    public BMPage getStartPage() {
        return startPage;
    }

    public void setStartPage(BMPage startPage) {
        this.startPage = startPage;
    }*/

}
