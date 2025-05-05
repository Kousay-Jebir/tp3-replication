package common.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class FileDataStore implements DataStore {
    private final Path filePath;

    public FileDataStore(String directory, String replicaId) {
        try {
            Path dirPath = Paths.get(directory);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }
            this.filePath = dirPath.resolve("replica_" + replicaId + ".txt");
            if (!Files.exists(filePath)) {
                Files.createFile(filePath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Impossible d'initialiser FileDataStore", e);
        }
    }

    @Override
    public synchronized void writeLine(String line) {
        try (BufferedWriter writer = Files.newBufferedWriter(
                filePath,
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND)) {
            writer.write(line);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException("Erreur lors de l’écriture dans le fichier", e);
        }
    }

    @Override
    public synchronized String readLastLine() {
        try {
            List<String> lines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
            if (lines.isEmpty()) {
                return null;
            }
            return lines.get(lines.size() - 1);
        } catch (IOException e) {
            throw new RuntimeException("Erreur lors de la lecture du fichier", e);
        }
    }

    @Override
    public synchronized List<String> readAllLines() {
        try {
            return new ArrayList<>(Files.readAllLines(filePath, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException("Erreur lors de la lecture du fichier", e);
        }
    }
}
