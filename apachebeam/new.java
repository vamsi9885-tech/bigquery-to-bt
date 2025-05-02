'bigtableRowKey': json_content.get("Rowkey"),
'bigtableColumns': json_content.get("columnNames"),  # now a JSON dict
'jsonContent': json.dumps(json_content),  # pass full mapping to Beam


String ColumnFamily = options.getBigtableColumnFamily();
String RowKey = options.getBigtableRowKey();
String TableColumns = options.getBigtableColumns();
String jsonContent = options.getJsonContent();
JSONObject mapping = new JSONObject(jsonContent);
JSONObject columnFamilyMap = mapping.getJSONObject("columnNames");
String[] rowKeyCols = mapping.getString("Rowkey").split(",");


StringBuilder rowKeyBuilder = new StringBuilder();
for (String keyCol : rowKeyCols) {
    keyCol = keyCol.trim();
    for (int i = 0; i < input_columns.length; i++) {
        if (input_columns[i].equals(keyCol)) {
            rowKeyBuilder.append(input_rows[i]).append("_");
            break;
        }
    }
}
rowKeyBuilder.append(formattedDate);
String rowKeyDynamic = rowKeyBuilder.toString();
Put row = new Put(Bytes.toBytes(rowKeyDynamic));


for (int i = 0; i < input_columns.length; i++) {
    String column = input_columns[i];
    if (columnFamilyMap.has(column)) {
        String family = columnFamilyMap.getString(column);
        row.addColumn(
            Bytes.toBytes(family),
            Bytes.toBytes(column),
            timestamp,
            Bytes.toBytes(input_rows[i])
        );
    }
}


<dependency>
    <groupId>org.json</groupId>
    <artifactId>json</artifactId>
    <version>20210307</version>
</dependency>


  import org.json.JSONObject;
import com.opencsv.CSVReader;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;




