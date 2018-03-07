package bitmap;


import diskmgr.Page;
import global.Convert;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;

import java.io.IOException;
import java.util.ArrayList;

public class BitMapOperations implements GlobalConst {
    public ArrayList<Integer> getIndexedPostions(BitMapFile bitMapFile)
            throws IOException {
        ArrayList<Integer> positions = new ArrayList<Integer>();

        BitMapHeaderPage bitMapHeaderPage = bitMapFile.getHeaderPage();
        PageId nextPageId = bitMapHeaderPage.getNextPage();
        BMPage bmPage = new BMPage();
        int numBMPages = 0;

        while (nextPageId.pid != INVALID_PAGE) {
            pinPage(nextPageId, bmPage);
            int count = bmPage.getCount();
            int counter = (bmPage.getCount() + 32) / 32;
            int reservedPage = bmPage.getStartByte();
            for (int i = 0; i < counter; i++) {
                int nextStart = i * 4 + reservedPage;
                int valToTraverse = Convert.getIntValue(nextStart, bmPage.getPage());
                int currentPtr = i * 32;
                int bytePos = 0;
                while (valToTraverse != 0) {
                    if ((valToTraverse & 1) != 0) {
                        int tempValue = (currentPtr + bytePos)
                                + numBMPages * (bmPage.getAvailableMap());
                        positions.add(tempValue);
                    }
                    valToTraverse = valToTraverse >> 1;
                    bytePos++;
                }
                currentPtr += 32;
            }
            numBMPages++;
            unpinPage(nextPageId, false);
            nextPageId.pid = bmPage.getNextPage().pid;
        }
        return positions;
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