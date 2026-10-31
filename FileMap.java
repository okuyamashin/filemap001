import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FileMap is a Map implementation that persists data to the filesystem.
 * Each key-value pair is stored as a separate file in a designated directory.
 * This implementation supports serializable keys and values.
 * 
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public class FileMap<K, V> implements Map<K, V>, Serializable {
    
    private static final long serialVersionUID = 1L;
    private final Path storageDirectory;
    private final Map<K, String> keyToFileMap;
    
    /**
     * Constructs a new FileMap with the default storage directory.
     * The default directory is ".filemap" in the current working directory.
     * 
     * @throws IOException if the storage directory cannot be created
     */
    public FileMap() throws IOException {
        this(Paths.get(".filemap"));
    }
    
    /**
     * Constructs a new FileMap with the specified storage directory.
     * 
     * @param storageDirectory the directory where files will be stored
     * @throws IOException if the storage directory cannot be created
     */
    public FileMap(Path storageDirectory) throws IOException {
        this.storageDirectory = storageDirectory;
        this.keyToFileMap = new ConcurrentHashMap<>();
        
        // Create storage directory if it doesn't exist
        if (!Files.exists(storageDirectory)) {
            Files.createDirectories(storageDirectory);
        }
        
        // Load existing keys from the storage directory
        loadExistingKeys();
    }
    
    /**
     * Constructs a new FileMap with the specified storage directory path.
     * 
     * @param directoryPath the path to the directory where files will be stored
     * @throws IOException if the storage directory cannot be created
     */
    public FileMap(String directoryPath) throws IOException {
        this(Paths.get(directoryPath));
    }
    
    /**
     * Loads existing keys from the storage directory.
     */
    private void loadExistingKeys() throws IOException {
        if (!Files.exists(storageDirectory) || !Files.isDirectory(storageDirectory)) {
            return;
        }
        
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDirectory, "*.dat")) {
            for (Path entry : stream) {
                try {
                    String fileName = entry.getFileName().toString();
                    if (fileName.endsWith(".dat")) {
                        // Read the key from the file
                        try (ObjectInputStream ois = new ObjectInputStream(
                                new BufferedInputStream(Files.newInputStream(entry)))) {
                            @SuppressWarnings("unchecked")
                            K key = (K) ois.readObject();
                            keyToFileMap.put(key, fileName);
                        } catch (ClassNotFoundException e) {
                            // Skip files that cannot be deserialized
                        }
                    }
                } catch (IOException e) {
                    // Skip problematic files
                }
            }
        }
    }
    
    /**
     * Generates a filename for a given key.
     * 
     * @param key the key
     * @return the filename
     */
    private String generateFileName(K key) {
        return keyToFileMap.computeIfAbsent(key, k -> 
            UUID.randomUUID().toString() + ".dat"
        );
    }
    
    /**
     * Gets the file path for a given key.
     * 
     * @param key the key
     * @return the file path
     */
    private Path getFilePath(K key) {
        String fileName = keyToFileMap.get(key);
        if (fileName == null) {
            fileName = generateFileName(key);
        }
        return storageDirectory.resolve(fileName);
    }
    
    @Override
    public int size() {
        return keyToFileMap.size();
    }
    
    @Override
    public boolean isEmpty() {
        return keyToFileMap.isEmpty();
    }
    
    @Override
    public boolean containsKey(Object key) {
        return keyToFileMap.containsKey(key);
    }
    
    @Override
    public boolean containsValue(Object value) {
        for (K key : keyToFileMap.keySet()) {
            V storedValue = get(key);
            if (Objects.equals(value, storedValue)) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public V get(Object key) {
        if (!keyToFileMap.containsKey(key)) {
            return null;
        }
        
        Path filePath = getFilePath((K) key);
        
        if (!Files.exists(filePath)) {
            keyToFileMap.remove(key);
            return null;
        }
        
        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(Files.newInputStream(filePath)))) {
            // Read and discard the key
            ois.readObject();
            // Read and return the value
            @SuppressWarnings("unchecked")
            V value = (V) ois.readObject();
            return value;
        } catch (IOException | ClassNotFoundException e) {
            return null;
        }
    }
    
    @Override
    public V put(K key, V value) {
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }
        
        V oldValue = get(key);
        Path filePath = getFilePath(key);
        
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new BufferedOutputStream(Files.newOutputStream(filePath, 
                    StandardOpenOption.CREATE, 
                    StandardOpenOption.TRUNCATE_EXISTING)))) {
            oos.writeObject(key);
            oos.writeObject(value);
            oos.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to file: " + filePath, e);
        }
        
        return oldValue;
    }
    
    @Override
    public V remove(Object key) {
        if (!keyToFileMap.containsKey(key)) {
            return null;
        }
        
        V oldValue = get(key);
        Path filePath = getFilePath((K) key);
        
        try {
            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            // Ignore deletion errors
        }
        
        keyToFileMap.remove(key);
        return oldValue;
    }
    
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }
    
    @Override
    public void clear() {
        // Delete all files
        for (K key : new ArrayList<>(keyToFileMap.keySet())) {
            remove(key);
        }
        keyToFileMap.clear();
    }
    
    @Override
    public Set<K> keySet() {
        return new HashSet<>(keyToFileMap.keySet());
    }
    
    @Override
    public Collection<V> values() {
        List<V> values = new ArrayList<>();
        for (K key : keyToFileMap.keySet()) {
            V value = get(key);
            values.add(value);
        }
        return values;
    }
    
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        Set<Map.Entry<K, V>> entries = new HashSet<>();
        for (K key : keyToFileMap.keySet()) {
            V value = get(key);
            entries.add(new AbstractMap.SimpleEntry<>(key, value));
        }
        return entries;
    }
    
    /**
     * Gets the storage directory path.
     * 
     * @return the storage directory path
     */
    public Path getStorageDirectory() {
        return storageDirectory;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (Map.Entry<K, V> entry : entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue());
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }
    
    /**
     * Example usage and testing.
     */
    public static void main(String[] args) {
        try {
            System.out.println("FileMap Demo - A Map implementation backed by files");
            System.out.println("==================================================");
            
            // Create a FileMap instance
            FileMap<String, String> map = new FileMap<>("demo_storage");
            
            // Clear any existing data
            map.clear();
            
            // Test basic operations
            System.out.println("\n1. Testing basic put and get operations:");
            map.put("name", "John Doe");
            map.put("email", "john@example.com");
            map.put("city", "New York");
            System.out.println("   Put 3 entries");
            System.out.println("   Size: " + map.size());
            System.out.println("   Get 'name': " + map.get("name"));
            System.out.println("   Get 'email': " + map.get("email"));
            
            // Test containsKey
            System.out.println("\n2. Testing containsKey:");
            System.out.println("   Contains 'name': " + map.containsKey("name"));
            System.out.println("   Contains 'age': " + map.containsKey("age"));
            
            // Test containsValue
            System.out.println("\n3. Testing containsValue:");
            System.out.println("   Contains value 'John Doe': " + map.containsValue("John Doe"));
            System.out.println("   Contains value 'Jane Doe': " + map.containsValue("Jane Doe"));
            
            // Test keySet
            System.out.println("\n4. Testing keySet:");
            System.out.println("   Keys: " + map.keySet());
            
            // Test values
            System.out.println("\n5. Testing values:");
            System.out.println("   Values: " + map.values());
            
            // Test entrySet
            System.out.println("\n6. Testing entrySet:");
            for (Map.Entry<String, String> entry : map.entrySet()) {
                System.out.println("   " + entry.getKey() + " -> " + entry.getValue());
            }
            
            // Test update
            System.out.println("\n7. Testing update (replace existing key):");
            String oldValue = map.put("name", "Jane Smith");
            System.out.println("   Old value for 'name': " + oldValue);
            System.out.println("   New value for 'name': " + map.get("name"));
            
            // Test remove
            System.out.println("\n8. Testing remove:");
            String removed = map.remove("city");
            System.out.println("   Removed 'city': " + removed);
            System.out.println("   Size after removal: " + map.size());
            
            // Test putAll
            System.out.println("\n9. Testing putAll:");
            Map<String, String> newEntries = new HashMap<>();
            newEntries.put("country", "USA");
            newEntries.put("phone", "123-456-7890");
            map.putAll(newEntries);
            System.out.println("   Size after putAll: " + map.size());
            
            // Test persistence (create new instance)
            System.out.println("\n10. Testing persistence:");
            System.out.println("    Creating new FileMap instance with same storage...");
            FileMap<String, String> map2 = new FileMap<>("demo_storage");
            System.out.println("    Size of new instance: " + map2.size());
            System.out.println("    Get 'name' from new instance: " + map2.get("name"));
            System.out.println("    Get 'email' from new instance: " + map2.get("email"));
            
            // Test toString
            System.out.println("\n11. Testing toString:");
            System.out.println("    " + map2.toString());
            
            // Test isEmpty
            System.out.println("\n12. Testing isEmpty:");
            System.out.println("    Is empty: " + map2.isEmpty());
            
            // Test clear
            System.out.println("\n13. Testing clear:");
            map2.clear();
            System.out.println("    Size after clear: " + map2.size());
            System.out.println("    Is empty: " + map2.isEmpty());
            
            System.out.println("\n==================================================");
            System.out.println("Demo completed successfully!");
            System.out.println("Storage directory: " + map.getStorageDirectory().toAbsolutePath());
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
