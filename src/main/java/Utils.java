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
    private static final String CSV_INPUT_INITIATORS_ACTIVE = "lawmaking_hackathon_initiators_active.csv";
    private static final int INITIATORS_ID_INDEX = 0;

    public static void main(String[] args) {
        processLawsToInitiators();
        processInitiatorsToPJA();
    }

    private static void processLawsToInitiators() {
        final String csvInput = "lawmaking_hackathon.csv";
        final String csvOutputSplit = "lawmaking_hackathon_split.csv";
        final String csvOutputPairs = "lawmaking_hackathon_pairs.csv";
        final int lawIdIndex = 0;
        final int lawInitiatorsIndex = 1;

        Map<String, Set<String>> idToInitiators = getIdToList(csvInput, lawIdIndex, lawInitiatorsIndex, "^\\['|']$", "', *'");

        idToInitiators = filterActiveInValues(idToInitiators);

        split(idToInitiators, csvOutputSplit);

        pairs(idToInitiators, csvOutputPairs);
    }

    private static void processInitiatorsToPJA() {
        final String csvInput = "lawmaking_hackathon_v2.csv";
        final String csvOutputSplit = "lawmaking_hackathon_split_pja.csv";
        final int initiatorIdIndex = 0;
        final int initiatorPJAIndex = 2;

        Map<String, Set<String>> initiatorToPJA = getIdToList(csvInput, initiatorIdIndex, initiatorPJAIndex, "^\\[|]$", ", *");

        initiatorToPJA = filterActiveInKeys(initiatorToPJA);

        split(initiatorToPJA, csvOutputSplit);
    }

    private static Map<String, Set<String>> getIdToList(String csvInputFile, int idIndex, int listIndex, String suffix, String splitBy) {
        Map<String, Set<String>> map = new HashMap<>();

        try (CSVReader reader = new CSVReader(new FileReader(csvInputFile))) {
            String[] line;
            int i = 0;
            while ((line = reader.readNext()) != null) {
                if (i++ == 0) continue;

                String id = line[idIndex];
                HashSet<String> initiators = new HashSet<>(asList(
                        line[listIndex]
                                .replaceAll(suffix, "")
                                .split(splitBy)));

                if (map.containsKey(id))
                    map.get(id).addAll(initiators);
                else map.put(id, initiators);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    private static Set<String> getInitiatorsActive() {
        Set<String> set = new HashSet<>();

        try (CSVReader reader = new CSVReader(new FileReader(CSV_INPUT_INITIATORS_ACTIVE))) {
            String[] line;
            int i = 0;
            while ((line = reader.readNext()) != null) {
                if (i++ == 0) continue;
                set.add(line[INITIATORS_ID_INDEX]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return set;
    }

    private static Map<String, Set<String>> filterActiveInValues(Map<String, Set<String>> idToInitiators) {
        Set<String> initiatorsActive = getInitiatorsActive();

        idToInitiators = EntryStream.of(idToInitiators)
                .mapValues(is -> {
                    is.retainAll(initiatorsActive);
                    return is;
                })
                .filterValues(is -> !is.isEmpty())
                .toMap();
        return idToInitiators;
    }

    private static Map<String, Set<String>> filterActiveInKeys(Map<String, Set<String>> initiatorsTo) {
        Set<String> initiatorsActive = getInitiatorsActive();

        initiatorsTo = EntryStream.of(initiatorsTo)
                .filterKeys(initiatorsActive::contains)
                .toMap();
        return initiatorsTo;
    }

    private static void pairs(Map<String, Set<String>> idToInitiators, String csvOutputFilename) {

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

        try (CSVWriter writer = new CSVWriter(new FileWriter(csvOutputFilename))) {
            EntryStream.of(initiatorPairToCommonProjects)
                    .mapValues(Set::size)
                    .forKeyValue((k, v) -> writer.writeNext(new String[]{String.valueOf(k), String.valueOf(v)}, false));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void split(Map<String, Set<String>> map, String csvOutputFilename) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvOutputFilename))) {
            EntryStream.of(map)
                    .flatMapValues(Set::stream)
                    .forKeyValue((k, v) -> writer.writeNext(new String[]{k, v}, false));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
