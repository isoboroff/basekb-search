package gov.nist.basekb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;

/**
 * Created by soboroff on 8/15/16.
 */
public class QueryLabelDb {
    public static void main(String[] args) {
        RocksDB.loadLibrary();
        Options rockopts = new Options().setCreateIfMissing(false);
        RocksDB db = null;

        try {
            db = RocksDB.open(rockopts, args[0]);
            String query = args[1];
            byte[] val = db.get(query.getBytes(StandardCharsets.UTF_8));
            if (val != null) {
                System.out.println(new String(val, StandardCharsets.UTF_8));
            }
            db.close();
        } catch (RocksDBException rdbe) {
            rdbe.printStackTrace(System.err);
        }
    }
}
