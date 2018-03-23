package bitmap;

import diskmgr.Page;
import global.Convert;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;

import java.io.IOException;
import java.util.ArrayList;

public class BitMapOperations implements GlobalConst {
	private int pos = 0;
	private int bitPos = 0;
	private int numBMPages = 0;
	private PageId nPageId = new PageId();
	private BitMapHeaderPage bitMapHeaderPage;
	private BMPage bmPage;
	private short valToTraverse;

	public ArrayList<Integer> getIndexedPostions(BitMapFile bitMapFile) throws IOException {
		ArrayList<Integer> positions = new ArrayList<Integer>();

		BitMapHeaderPage bitMapHeaderPage = bitMapFile.getBitMapHeaderPage();
		pinPage(bitMapFile.getHeaderPageId(), bitMapHeaderPage);
		PageId nextPageId = bitMapHeaderPage.getNextPage();
		unpinPage(bitMapFile.getHeaderPageId(), false);
		BMPage bmPage = new BMPage();
		int numBMPages = 0;

		while (nextPageId.pid != INVALID_PAGE) {
			pinPage(nextPageId, bmPage);
			int counter = bmPage.getCount();
			int reservedPage = bmPage.getStartByte();
			for (int i = 0; i < counter; i++) {
				int nextStart = i + reservedPage;
				short valToTraverse = bmPage.getPage()[nextStart];
				int currentPtr = i * 8;
				int bytePos = 0;

				for (int k = 0; k < 8; k++) {
					if ((valToTraverse & 1) != 0) {
						int tempValue = (currentPtr + bytePos) + numBMPages * (bmPage.getAvailableMap() * 8);
						positions.add(tempValue);
						// System.out.println(tempValue);
					}
					valToTraverse = (short) (valToTraverse >> 1);
					bytePos++;
				}
			}
			numBMPages++;
			unpinPage(nextPageId, false);
			nextPageId.pid = bmPage.getNextPage().pid;
		}
		return positions;
	}

	public int getNextIndexedPosition() throws IOException {

		while (nPageId.pid != INVALID_PAGE) {
			int counter = bmPage.getCount();
			int reservedPage = bmPage.getStartByte();
			for (int i = pos; i < counter; i++) {
				int nextStart = i + reservedPage;
				valToTraverse = bmPage.getPage()[nextStart];
				valToTraverse = (short) (valToTraverse >> bitPos);
				int currentPtr = i * 8;

				for (int k = bitPos; k < 8; k++) {
					if ((valToTraverse & 1) != 0) {
						int tempValue = (currentPtr + bitPos) + numBMPages * (bmPage.getAvailableMap() * 8);
						pos = tempValue / 8;
						bitPos = (tempValue % 8) + 1;
						return tempValue;
					}
					valToTraverse = (short) (valToTraverse >> 1);
					bitPos++;
				}
				pos++;
				bitPos = 0;
			}
			numBMPages++;
			// numBitsInPage = numBitsInPage + counter*8;
			pos = 0; // // When the data is exceeded to the next page.
			unpinPage(nPageId, false);
			nPageId.pid = bmPage.getNextPage().pid;
		}
		return -1;
	}

	public void init(BitMapFile bitMapFile) throws IOException {
		bitMapHeaderPage = bitMapFile.getBitMapHeaderPage();
		bmPage = new BMPage();
		nPageId = bitMapHeaderPage.getNextPage();
		pinPage(nPageId, bmPage);
		unpinPage(bitMapFile.getHeaderPageId(), false);
		nPageId = bitMapHeaderPage.getNextPage();
	}

	public void pinPage(PageId pageId, Page page) {
		try {
			SystemDefs.JavabaseBM.pinPage(pageId, page, false);
		} catch (Exception e) {

		}
	}

	public void unpinPage(PageId pageId, boolean dirty) {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageId, dirty);
		} catch (Exception e) {

		}
	}

}