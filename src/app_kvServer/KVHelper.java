package app_kvServer;


import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

public class KVHelper {

    private ConcurrentHashMap<String, String> Cache;
    private final int NUM_PERSISTENT_STORES = 8;
    private final int CACHE_LIMIT = 2;

    private void writeToFile(ArrayList<String> KVs, int fileIndex) {
        String fileName = "store" + (fileIndex + 1) + ".txt";
        try (FileWriter fw = new FileWriter(fileName, true))
        {
            BufferedWriter bw = new BufferedWriter(fw);
            for (int i = 0; i < KVs.size(); i+=2) {
                String hashedKey = String.valueOf(KVs.get(i).hashCode());
                bw.write(hashedKey + "=" + KVs.get(i+1));
                bw.newLine();
            }
            bw.flush();
            bw.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

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

        ArrayList<String>[] KVGroupings = new ArrayList[NUM_PERSISTENT_STORES];

        for (int i = 0; i < NUM_PERSISTENT_STORES; i++) {
            KVGroupings[i] = new ArrayList<String>();
        }

        while (itr.hasNext()) {
            Map.Entry<String, String> entry = itr.next();
            String key = entry.getKey();
            String value = entry.getValue();
            int fileIndex = getFileIndex(key);

            KVGroupings[fileIndex].add(key);
            KVGroupings[fileIndex].add(value);

            itr.remove();
        }

        for (int i = 0; i < NUM_PERSISTENT_STORES; i++) {
            if (!KVGroupings[i].isEmpty())
                writeToFile(KVGroupings[i], i);
        }
    }

    private int getFileIndex(String key) {
        return (int) (key.charAt(0))  % NUM_PERSISTENT_STORES + 1;
    }

    private String readFromFile(String key) {
        String value = null;
        try {
            int fileIndex = getFileIndex(key);
            String fileName = "store" + (fileIndex + 1) + ".txt";
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
        Cache = new ConcurrentHashMap<String, String>();
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
