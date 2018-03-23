package bitmap;

import columnar.ColumnarFile;
import columnar.ColumnarFilePinPageException;
import columnar.ColumnarFileUnpinPageException;
import diskmgr.Page;
import global.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class BitMapTest {
	@Before
	public void setup() {

	}

	private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	public String randomAlphaNumeric(int count) {
		StringBuilder builder = new StringBuilder();
		while (count-- != 0) {
			int character = (int) (Math.random() * ALPHA_NUMERIC_STRING.length());
			builder.append(ALPHA_NUMERIC_STRING.charAt(character));
		}
		return builder.toString();

	}

	@Test
	public void setupBitMapOperation() throws Exception {
		String dbPath = "Minibase.min";
		SystemDefs systemDefs = new SystemDefs(dbPath, 20000, 10, null);
		AttrType[] attrTypes = new AttrType[20];
		String[][] in = new String[10000][20];
		int[] sizes = new int[20];
		for (int i = 0; i < 20; i++) {
			attrTypes[i] = new AttrType();
			attrTypes[i].setColumnId(i);
			attrTypes[i].setSize(12);
			attrTypes[i].setAttrType(0);
			attrTypes[i].setAttrName("Column" + i);
			sizes[i] = 12;
		}
		ColumnarFile columnarFile = new ColumnarFile("Employee", 20, attrTypes);

		int min = Integer.MAX_VALUE;
		for (int i = 0; i < 10000; i++) {
			for (int j = 0; j < 20; j++) {
				if (i % 10 == 0)
					in[i][j] = randomAlphaNumeric(10);
				else {
					in[i][j] = "Shashank";
				}
			}
			try {
				columnarFile.insertTuple(Convert.stringToByteA(in[i], sizes));

			} catch (Exception e) {
				System.out.println();
			}
		}
		SystemDefs.JavabaseBM.flushAllPages();
		columnarFile.createBitMapIndex(3, new StringValue("Shashank"));
		SystemDefs.JavabaseBM.flushAllPages();
		BitMapFile bitMapFile = new BitMapFile("Employee.3.Shashank");
		SystemDefs.JavabaseBM.flushAllPages();
		BitMapOperations bitMapOperations = new BitMapOperations();
		SystemDefs.JavabaseBM.flushAllPages();
		bitMapFile.Delete(9999);
		SystemDefs.JavabaseBM.flushAllPages();

		bitMapFile.Insert(10000);
		SystemDefs.JavabaseBM.flushAllPages();

		bitMapOperations.getIndexedPostions(bitMapFile);
		SystemDefs.JavabaseBM.flushAllPages();

	}

	public void scanBitMapTest() throws Exception {
		String dbPath = "Minibase.min";
		SystemDefs systemDefs = new SystemDefs(dbPath, 20000, 10, null);
		AttrType[] attrTypes = new AttrType[20];
		String[][] in = new String[100][20];
		int[] sizes = new int[20];
		for (int i = 0; i < 20; i++) {
			attrTypes[i] = new AttrType();
			attrTypes[i].setColumnId(i);
			attrTypes[i].setSize(12);
			attrTypes[i].setAttrType(0);
			attrTypes[i].setAttrName("Column" + i);
			sizes[i] = 12;
		}
		ColumnarFile columnarFile = new ColumnarFile("Employee", 20, attrTypes);
		ArrayList<Integer> outPutarrList = new ArrayList<>();
		ArrayList<Integer> inPutarrList = new ArrayList<>();
		BitMapFile bitMapFile = new BitMapFile("Emp", 100);
		BMPage bmPage = new BMPage();
		Page apage = new Page();
		// PageId pageId = new PageId();
		BitMapHeaderPage p = bitMapFile.getBitMapHeaderPage();
		pinPage(bitMapFile.getHeaderPageId(), p);
		PageId pageid1 = SystemDefs.JavabaseBM.newPage(apage, 1);
		bitMapFile.allocatePage(pageid1, 1);
		p.setNextPage(pageid1);
		pinPage(pageid1, bmPage);
		bmPage.init(pageid1, bmPage);

		unpinPage(bitMapFile.getHeaderPageId(), true);
		bitMapFile.Insert(6);
		bitMapFile.Insert(13);
		bitMapFile.Insert(15);
		bitMapFile.Insert(22);
		bitMapFile.Insert(30);
		inPutarrList.add(6);
		inPutarrList.add(13);
		inPutarrList.add(15);
		inPutarrList.add(22);
		inPutarrList.add(30);
		inPutarrList.add(-1);
		unpinPage(pageid1, true);
		BitMapOperations b = new BitMapOperations();
		BitMapOperations bitMapOperations = new BitMapOperations();
		bitMapOperations.init(bitMapFile);
		int resultVal = Integer.MIN_VALUE;
		while (resultVal != -1) {
			resultVal = bitMapOperations.getNextIndexedPosition();
			outPutarrList.add(resultVal);
		}
		boolean result = inPutarrList.equals(outPutarrList);
		System.out.println(result);
	}

	private void pinPage(PageId pageId, Page page) throws ColumnarFilePinPageException {
		try {
			SystemDefs.JavabaseBM.pinPage(pageId, page, false);
		} catch (Exception e) {
			throw new ColumnarFilePinPageException(e, "Columnar: Not able to pin page");
		}
	}

	private void unpinPage(PageId pageId, boolean dirty) throws ColumnarFileUnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageId, dirty);
		} catch (Exception e) {
			throw new ColumnarFileUnpinPageException(e, "Columnar: not able to unpin");
		}
	}

	@After
	public void cleanUp() {

	}

}
