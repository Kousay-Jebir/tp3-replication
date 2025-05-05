package common.storage;

import java.util.List;

public interface DataStore {

    void writeLine(String line);

    String readLastLine();

    List<String> readAllLines();
}
