import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import one.util.streamex.EntryStream;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class Utils {
    private static final String CSV_INPUT = "lawmaking_hackathon.csv";
    private static final String CSV_OUTPUT_SPLIT = "lawmaking_hackathon_split.csv";
    private static final String CSV_OUTPUT_PAIRS = "lawmaking_hackathon_pairs.csv";
    private static final int LAW_ID_INDEX = 0;
    private static final int LAW_INITIATORS_INDEX = 1;

    public static void main(String[] args) {
        Map<String, Set<String>> idToInitiators = getIdToInitiators();

        split(idToInitiators);

        pairs(idToInitiators);

    }

    private static Map<String, Set<String>> getIdToInitiators() {
        Map<String, Set<String>> map = new HashMap<>();

        try (CSVReader reader = new CSVReader(new FileReader(CSV_INPUT))) {
            String[] line;
            int i = 0;
            while ((line = reader.readNext()) != null) {
                if (i++ == 0) continue;

                String id = line[LAW_ID_INDEX];
                HashSet<String> initiators = new HashSet<>(asList(
                        line[LAW_INITIATORS_INDEX]
                                .replaceAll("^\\['|']", "")
                                .split("', *'")));

                if (map.containsKey(id))
                    map.get(id).addAll(initiators);
                else map.put(id, initiators);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    private static void pairs(Map<String, Set<String>> idToInitiators) {

        Map<Set<String>, Set<String>> initiatorPairToCommonProjects = new HashMap<>();

        idToInitiators.forEach((id, initiators) -> {
            for (String i1 : initiators) {
                for (String i2 : initiators) {
                    if (!i1.equals(i2)) {
                        initiatorPairToCommonProjects.merge(
                                new HashSet<>(asList(i1, i2)),
                                new HashSet<>(singletonList(id)),
                                (old, add) -> {
                                    old.addAll(add);
                                    return old;
                                });
                    }
                }
            }
        });

        try (CSVWriter writer = new CSVWriter(new FileWriter(CSV_OUTPUT_PAIRS))) {
            EntryStream.of(initiatorPairToCommonProjects)
                    .mapValues(Set::size)
                    .forKeyValue((k, v) -> writer.writeNext(new String[]{String.valueOf(k), String.valueOf(v)}, false));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void split(Map<String, Set<String>> map) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(CSV_OUTPUT_SPLIT))) {
            EntryStream.of(map)
                    .flatMapValues(Set::stream)
                    .forKeyValue((k, v) -> writer.writeNext(new String[]{k, v}, false));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
