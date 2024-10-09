package de.hpi.isg;

public class ConfigParameter {
    static String dataset = "twitter";
    static String configPath = "/home/y/neu/erasure/";
    static String ruleFile = "rules_" + dataset + ".csv";
    static String schemaFile = "schema_" + dataset + ".csv";
    static String derivedFile = "derived_" + dataset + ".csv";
    static String resultFile = "result_" + dataset + ".db";

    static String connectionUrl = "jdbc:postgresql://localhost:5432/"; // Note final slash
    static String database = "obliviatortest";
    static String username = "postgres";
    static String password = "postgres";
    static int numKeys = 10;
    static boolean batching = false;
    static boolean scheduling = false;
    static int[] batchSizes = new int[]{numKeys};
    static boolean isBatchSizeTime = false;


    public static void setDataset(String dataset) {
        ConfigParameter.dataset = dataset;
        ConfigParameter.ruleFile = "rules_" + dataset + ".csv";
        ConfigParameter.schemaFile = "schema_" + dataset + ".csv";
        ConfigParameter.derivedFile = "derived_" + dataset + ".csv";
        ConfigParameter.resultFile = "result_" + dataset + ".db";
    }
}
