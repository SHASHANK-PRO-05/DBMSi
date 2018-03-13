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
                        int tempValue = (currentPtr + bytePos)
                                + numBMPages * (bmPage.getAvailableMap() * 8);
                        positions.add(tempValue);

                    }
                    valToTraverse = (short) (valToTraverse >> 1);
                    bytePos++;
                }
            }
            numBMPages++;
            nextPageId.pid = bmPage.getNextPage().pid;
            unpinPage(nextPageId, false);
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