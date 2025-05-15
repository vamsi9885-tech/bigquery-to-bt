public static Dataset<Row> fetchBigQueryData(SparkSession spark, String projectId, String datasetId, JSONObject dictResults) {
    // Extract bq_table_name
    String bqTableName = dictResults.getJSONObject("table").getString("bq_table_name");

    // Define excluded columns
    Set<String> excludedColumns = Set.of("bq_rowkey", "cm13", "cm15", "decrypt_cm13", "decrypt_cm15");

    // Extract column_mapping and filter keys
    JSONObject columnMapping = dictResults.getJSONObject("column_mapping");
    List<String> columnKeys = new ArrayList<>();
    for (String key : columnMapping.keySet()) {
        if (!excludedColumns.contains(key)) {
            columnKeys.add(key);
        }
    }

    String selectedColumns = String.join(", ", columnKeys);

    // Format decryption expressions
    JSONObject decryptExprs = dictResults.getJSONObject("decrypt_expr");
    Map<String, String> decryptExpr = new HashMap<>();
    for (String key : decryptExprs.keySet()) {
        String expr = decryptExprs.getString(key)
                .replace("{project_id}", projectId)
                .replace("{dataset_id}", datasetId)
                .replace("{bq_table_name}", bqTableName);
        decryptExpr.put(key, expr);
    }

    String decrypt_cm13 = decryptExpr.get("decrypt_cm13");
    String decrypt_cm15 = decryptExpr.get("decrypt_cm15");

    // Format query
    String queryTemplate = dictResults.getString("query");
    String query = queryTemplate
            .replace("{decrypt_cm13}", decrypt_cm13)
            .replace("{decrypt_cm15}", decrypt_cm15)
            .replace("{columns}", selectedColumns)
            .replace("{project_id}", projectId)
            .replace("{dataset_id}", datasetId)
            .replace("{bq_table_name}", bqTableName);

    System.out.println("Executing this query: " + query);

    // Load data from BigQuery
    Dataset<Row> df = spark.read()
            .format("bigquery")
            .option("query", query)
            .load()
            .cache();

    // Log details
    System.out.println("Fetched " + df.count() + " rows from table " + bqTableName);
    System.out.println("Total records fetched from BigQuery: " + df.count());
    System.out.println("Number of partitions: " + df.rdd().getNumPartitions());
    df.printSchema();

    return df;
}
