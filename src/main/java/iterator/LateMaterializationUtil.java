package iterator;

import bitmap.BitMapFile;

import java.util.ArrayList;

abstract interface LateMaterializationUtil {
    abstract void destroyEveryThing() throws Exception;

    abstract ArrayList<BitMapFile>[] makeBitMapFile() throws Exception;
}
