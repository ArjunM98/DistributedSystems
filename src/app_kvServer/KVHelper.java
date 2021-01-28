package app_kvServer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

public class KVHelper {

    private HashMap<String, String> Cache;
    private final int NUM_PERSISTENT_STORES = 8;
    private final int CACHE_LIMIT = 2;

    //TODO: maybe return enum here (more informative)?
    private void writeToFile(String key, String value) {
        try {
            int fileIndex = getFileIndex(key);
            String fileName = "store" + fileIndex + ".txt";
            File store = new File(fileName);
            if (store.createNewFile()) {
                System.out.println("Store created: " + store.getName());
            }
            try {
                FileWriter writer = new FileWriter(fileName);

                String hashedKey = String.valueOf(key.hashCode());

                writer.write(hashedKey + "=" + value);
                writer.close();
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    //TODO: group entries going to same file and use bufferedwriter for better performance
    private void purge() {
        Iterator<Map.Entry<String, String>> itr = Cache.entrySet().iterator();

        while (itr.hasNext()) {
            Map.Entry<String, String> entry = itr.next();
            String key = entry.getKey();
            String value = entry.getValue();
            writeToFile(key, value);
            itr.remove();
        }
    }

    private int getFileIndex(String key) {
        return (int) (key.charAt(0))  % NUM_PERSISTENT_STORES + 1;
    }

    private String readFromFile(String key) {
        String value = null;
        try {
            int fileIndex = getFileIndex(key);
            String fileName = "store" + fileIndex + ".txt";
            File store = new File(fileName);
            Scanner reader = new Scanner(store);

            String hashedKey = String.valueOf(key.hashCode());

            while (reader.hasNextLine()) {
                String data = reader.nextLine();
                if (data.contains(hashedKey)) {
                    value = data.substring(data.indexOf("=") + 1);
                }
            }
            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return value;
    }

    public String get(String key) {
        String value = null;
        String cachedValue = Cache.get(key);
        if (cachedValue == null) {
            value = readFromFile(key);
        } else {
            value = cachedValue;
        }
        return value;
    }

    //TODO: maybe return enum here (more informative)?
    public void put(String key, String value) {
        Cache.put(key, value);
        if (Cache.size() >= CACHE_LIMIT) {
            purge();
        }
    }


    //TODO: add destructor that purges cache

    public KVHelper() {
        Cache = new HashMap<String, String>();
    }

    public static void main(String[] args) {
        KVHelper helper = new KVHelper();
        helper.put("hello", "world");
        helper.put("ECE", "419");
        helper.put("TEP", "322");
        helper.put("ECE", "420");
        helper.put("something", "new");
        System.out.println(helper.get("hello"));
        System.out.println(helper.get("ECE"));
    }

}
