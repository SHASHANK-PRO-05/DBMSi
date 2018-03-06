package bitMap;
import global.PageId;
import heap.*;

//The commented code is because of dependency on BMPage

public class BitMapHeaderPage extends HFPage{

    private int columnIndex;
    
   
    public BitMapHeaderPage(int columnIndex, BMPage startPage) {
        this.columnIndex = columnIndex;
        
    }


    public BitMapHeaderPage(PageId headerPageId) {
		// TODO Auto-generated constructor stub
	}

	public BitMapHeaderPage() {
		// TODO Auto-generated constructor stub
	}


	public int getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

	public PageId getPageId() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setValue(ValueClass value) {
		// TODO Auto-generated method stub
		
	}


}