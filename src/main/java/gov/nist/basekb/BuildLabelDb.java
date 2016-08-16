package gov.nist.basekb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by soboroff on 8/15/16.
 */
public class BuildLabelDb {
   public static void main(String[] args) {
        RocksDB.loadLibrary();
        Options rockopts = new Options().setCreateIfMissing(true);
        RocksDB db = null;
        int count = 0;

        try {
            BufferedReader in = new BufferedReader(new FileReader(args[0]));
            db = RocksDB.open(rockopts, args[1]);
            String line;

            while ((line = in.readLine()) != null) {
                String[] fields = line.split("\\t+");
                if (fields[2].endsWith("@en")) {
                    byte[] key = fields[0].substring(1, fields[0].length() - 1).getBytes(StandardCharsets.UTF_8);
                    byte[] value = fields[2].substring(1, fields[2].length() - 4).getBytes(StandardCharsets.UTF_8);
                    db.put(key, value);
                }
                count++;
                if ((count % 100000) == 0)
                    System.out.print(".");
            }

            db.close();
            System.out.println();
        } catch (RocksDBException rdbe) {
            rdbe.printStackTrace(System.err);
        } catch (IOException ioe) {
            ioe.printStackTrace(System.err);
        }
    }
}
