package gov.nist.basekb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;

/**
 * Created by soboroff on 8/15/16.
 */
public class DumpLabelDb {
    public static void main(String[] args) {
        RocksDB.loadLibrary();
        Options rockopts = new Options().setCreateIfMissing(false);
        RocksDB db = null;

        try {
            db = RocksDB.open(rockopts, args[0]);
            RocksIterator iter = db.newIterator();
            iter.seekToFirst();
            while (iter.isValid()) {
                String key = new String(iter.key(), StandardCharsets.UTF_8);
                String val = new String(iter.value(), StandardCharsets.UTF_8);
                System.out.println(key + " -> " + val);
                iter.next();
            }

            db.close();
        } catch (RocksDBException rdbe) {
            rdbe.printStackTrace(System.err);
        }
    }
}
