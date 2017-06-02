package com.csvAnalyzer.neilo;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

/**
 * Csv-file analyzer.
 */
public class ReviewsAnalyzer {

    public ReviewsAnalyzer() {
        MAX_THREADS_COUNT = Math.max(OPERATING_SYS.getAvailableProcessors() - 1, 1);
    }


    public static void main(String[] args) throws Exception {
        ReviewsAnalyzer analyzer = new ReviewsAnalyzer();
//        analyzer.start = System.currentTimeMillis();          // for test
//      String fileAdress = analyzer.askFile();  //not finished
        analyzer.proceedAnalysing();
    }

    /**
     * Ask user about ready fileName.cva throw console
     * @return file name
     */
    private String askFile() {
        String fileAdress = "";
        // method, which ask user to choose ready file.csv with data
        return fileAdress;
    }

    /**
     * Load and analysing process start
     * @throws Exception
     */
    private void proceedAnalysing() throws Exception {

        File file = new File(FILE_NAME);
        Csv.Reader reader = getReader(file);

        LinkedList<String> necessaryDataList = new LinkedList<String>();;
        List <String> rawDataList = new LinkedList<String>();
        boolean firstLine = true;

        try {
            while ( (rawDataList = reader.readLine()) != null
                                && recievedRecordCount < 10000  // for testing
                    ) {
                if (firstLine) {
                    if (checkDataStructure(rawDataList)) {
                        noDublicatesMap = new HashMap<String, HashMap<String, ArrayList<String>>>();
                        // TODO: put here all initialization of class variables
                        firstLine = false;
                    } else {
                        System.out.println("File has another data sequence. "
                                + "Check the file, please, or choose another one.");
                        reader.close();
                        return;
                    }
                } else {
                    necessaryDataList = new LinkedList<>();
                    necessaryDataList.add(rawDataList.get(fieldsPositions.get(FILE_NEEDED_STRUCTURE[0])));
                    necessaryDataList.add(rawDataList.get(fieldsPositions.get(FILE_NEEDED_STRUCTURE[1])));
                    necessaryDataList.add(rawDataList.get(fieldsPositions.get(FILE_NEEDED_STRUCTURE[3])));
                    necessaryDataList.add(rawDataList.get(fieldsPositions.get(FILE_NEEDED_STRUCTURE[2])));
                    synchronized(dataStorage) {
                        dataStorage.push(necessaryDataList);
                        recievedRecordCount++;
                    }
                    if (threadCount == 0 || (dataStorage.size() > MAX_UNHANDLED_ROW*threadCount
                            && threadCount < MAX_THREADS_COUNT)) {

                        //additional parameter take into account during creating or not new thread
                        // not finished
//                              double averSysLoad = OPERATING_SYS.getSystemLoadAverage();
                        startNewThread();
                        threadCount++;
                    }
                }
            }
        } catch (Exception ex) {
            throw new Exception(ex);
        } finally {
            reader.close();
            loadAlldata = true;
        }
    }

    /**
     * Check data structure: file should contained columns with special names
     * @param list list of columns name
     * @return true, if all necessary columns were found
     */
    private boolean checkDataStructure(List list) {
        int position;
        boolean ok = list != null;
        for (int i = 0; ok && i < FILE_NEEDED_STRUCTURE.length; i++) {
            position = list.indexOf(FILE_NEEDED_STRUCTURE[i]);
            if (position >= 0) {
                fieldsPositions.put(FILE_NEEDED_STRUCTURE[i], position);
            } else {
                ok = false;
            }
        }
        return ok;
    }

    /**
     * Start new analysing thread
     */
    private void startNewThread() {
        Thread thr = new Thread( new Runnable() {
            @Override
            public void run() {
                analyze();
            }
        });
        thr.setPriority(9);
        thr.start();
    }

    /**
     * Run analysing data
     */
    public void analyze() {
        while (true) {
            if (dataStorage.isEmpty()) {
                try {
                    if (loadAlldata) {
                        if (threadCount == 1) printResults();
                        threadCount--;
                        break;
                    } else {
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException ex) {
                    System.out.println("Error during Thread.sleep(): " + ex);
                }
            } else {
                saveNewRecord(extractRawRecord());
            }
        }
    }

    /**
     * Pop the record from data storage
     * @return list of data
     */
    private List<String> extractRawRecord() {
        List<String> data = null;
        synchronized(dataStorage) {
            if (!dataStorage.isEmpty()) {
                data = new LinkedList<>(dataStorage.pop());
                dataStorage.notifyAll();
            }
        }
        return data;
    }

    /**
     * Get new csv-file reader
     * Additional file was download as open-source unit.
     * link:     http://www.javenue.info/post/78
     * @param file name of file, which is need to read, which locate on root directory
     * @return fileReader
     * @throws FileNotFoundException
     */
    private Csv.Reader getReader(File file) throws FileNotFoundException {
        Csv.Reader reader = new Csv.Reader( new FileReader(file))
                .delimiter(',').preserveSpaces(false).ignoreComments(true);

        return reader;
    }

    /**
     * Save new received record
     * @param list raw data
     */
    private void saveNewRecord(List<String> list) {
        if (list == null || list.size() == 0) return;

        String productId = list.get(0);
        String userId = list.get(1);
        String comment = list.get(2);
        if (!contains(productId, userId, comment)) {
            // if it is new unique record - add it
            String profileName = list.get(3);
            dataHandling(mostActiveUsers, usersActivities, profileName, false);
            dataHandling(mostCommentedFoods, foodsCommenaties, productId, false);
            dataHandling(mostOftenUsedWords, usedWords, comment, true);
        }
    }

    /**
     * Check, does comment (comment) of user (with userId) about product
     * (with productId) the same as one of the previous comment
     * @param productId Id of product (source data)
     * @param userId user Id (source data)
     * @param comment comment, which looking for in previous comments
     * @return true, if this comment is already contained
     */
    private boolean contains(String productId, String userId,
                             String comment) {

        HashMap <String, ArrayList<String>> recordData = noDublicatesMap.get(userId);
        if (recordData == null) recordData = new HashMap();

        ArrayList<String> previousComments = recordData.get(productId);
        if (previousComments == null) previousComments = new ArrayList<>();
        boolean hasSuchComment = previousComments.contains(comment);

        if (!hasSuchComment) {
            // add new record
            previousComments.add(comment);
            recordData.put(productId, previousComments);
            noDublicatesMap.put(userId, recordData);
        }
        return hasSuchComment;
    }

    /**
     * @param mostSet set with most variables
     * @param handlMap map, which contained all the data
     * @param checkedParameter parameter, which is analysed
     * @param isWordsAnalysing flag, signal about analysinig comment words
     */
    private void dataHandling(TreeSet<String> mostSet, HashMap<String, Integer> handlMap,
                              String checkedParameter, boolean isWordsAnalysing) {
        synchronized (mostSet) {
            synchronized (handlMap) {
                if (isWordsAnalysing) {
                    for (String word: checkedParameter.split(" ")) {
                        word = checkWord(word);
                        putDataIntoMostMap(mostOftenUsedWords, usedWords, word);
                    }
                } else {
                    putDataIntoMostMap(mostSet, handlMap, checkedParameter);
                }
                handlMap.notifyAll();
            }
            mostSet.notifyAll();
        }
    }

    /**
     * Check the word: remove non letters symbol from start and end of the word
     * @param word analyset word
     * @return checked word
     */
    private String checkWord(String word) {
        while (word.matches("^[^a-zA-Z].*")) word = word.substring(1);
        while (word.endsWith("\"")) word = word.substring(0, word.length() - 1);
        return word;
    }

    /**
     * Analyze "most" parameter, and manage storage with "most"-Map
     * @param mostSet Set, which contained most values
     * @param handlMap Map, which contained all handled data
     * @param checkedParameter analyzed parameter
     */
    private void putDataIntoMostMap(TreeSet<String> mostSet,
                                    HashMap<String, Integer> handlMap, String checkedParameter) {
        if (checkedParameter == null) return;
        Integer counter = handlMap.get(checkedParameter);

        if (counter ==  null) counter = 0; // first occurrence
        handlMap.put(checkedParameter, ++counter);

        if (mostSet.size() <= MAX_COUNT_MOST_VALUES) {
            mostSet.add(checkedParameter);
        } else if (handlMap.get(mostSet.last()) < counter) {
            mostSet.remove(mostSet.last());
            mostSet.add(checkedParameter);
        }
    }

    /**
     * Start method to print obtained results
     */
    private void printResults() {
        printTheMost(mostActiveUsers, "1000 most active users (profile names): ");
        printTheMost(mostCommentedFoods, "1000 most commented food items (item ids): ");
        printTheMost(mostOftenUsedWords, "1000 most used words in the reviews: ");

        // for efficiency testing
//        long time = (System.currentTimeMillis() - start);
//        System.out.println("AvarageTime: " + time + " ms");
    }

    /**
     *  Print result data
     * @param mostTree tree, contained alphabetically ordered data
     * @param startText text to describe in general next results
     */
    private void printTheMost(TreeSet<String> mostTree, String startText) {
        System.out.println(startText);
        System.out.println("");
        for (String str : mostTree) {
            System.out.println(str);
        }
    }

    // Maps for handling data
    /** HashMap for handling user activities data */
    private HashMap<String, Integer> usersActivities = new HashMap<>();

    /** HashMap for handling food commentaries data */
    private HashMap<String, Integer> foodsCommenaties = new HashMap<>();

    /** HashMap for handling used words data */
    private HashMap<String, Integer> usedWords = new HashMap<>();

    // "most" Sets.
    /** Most active users set */
    private TreeSet<String> mostActiveUsers = new TreeSet<>();

    /** Most commented foods set */
    private TreeSet<String> mostCommentedFoods = new TreeSet<>();

    /** Most often used words set */
    private TreeSet<String> mostOftenUsedWords = new TreeSet<>();


    /** Intermediate data storage: between loading and analysing thread */
    private LinkedList<List<String>> dataStorage = new LinkedList<List<String>>();

    /** Map for analyze dublicates record. Exmp: userId, Map(productID, ArrayList of comments) */
    private HashMap<String, HashMap<String, ArrayList<String>>> noDublicatesMap;

    /** Hash Map to save position of necessary fields in source-file (column number) */
    private HashMap<String, Integer> fieldsPositions = new HashMap<>();

    /** Analysing thread counter */
    private Integer threadCount = 0;

    /** Flag, signal that all source-file data is obtained */
    private boolean loadAlldata = false;

    /** Number of "most" values in result report */
    private final int MAX_COUNT_MOST_VALUES = 1000;

    /** Max thread number - calculate base on available processor number */
    private final int MAX_THREADS_COUNT;

    /** Max unhandled items in dataStoge on every thread (indicates about possibility to start new thread) */
    private final int MAX_UNHANDLED_ROW = 10;


    /** Array of field names, which are looked for in the file */
    private final String[] FILE_NEEDED_STRUCTURE = {"ProductId", "UserId",
            "ProfileName", "Text"};

    /** Bean for analyse system available resources and capacity */
    private final OperatingSystemMXBean OPERATING_SYS = ManagementFactory.getOperatingSystemMXBean();

    /** Name of file, which needed to read. */
    private final String FILE_NAME = "Reviews.csv";

    //          FOR TESTING
    //    long start; // start-time for efficiency testing
    //    private int analysedRows = 0;   // counter of analysed rows
        int recievedRecordCount = 0;     //for tests
}
