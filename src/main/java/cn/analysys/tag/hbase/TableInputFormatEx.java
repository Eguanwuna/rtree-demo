package cn.analysys.tag.hbase;


import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TableInputFormatEx extends TableInputFormatBaseEx
        implements Configurable {

    @SuppressWarnings("hiding")
    private static final Log LOG = LogFactory.getLog(org.apache.hadoop.hbase.mapreduce.TableInputFormat.class);

    /**
     * Job parameter that specifies the input table.
     */
    public static final String INPUT_TABLE = "hbase.mapreduce.inputtable";
    /**
     * If specified, use start keys of this table to split.
     * This is useful when you are preparing data for bulkload.
     */
    private static final String SPLIT_TABLE = "hbase.mapreduce.splittable";
    /**
     * Base-64 encoded scanner. All other SCAN_ confs are ignored if this is specified.
     * See {@link TableMapReduceUtil#convertScanToString(Scan)} for more details.
     */
    public static final String SCAN = "hbase.mapreduce.scan";
    /**
     * Scan start row
     */
    public static final String SCAN_ROW_START = "hbase.mapreduce.scan.row.start";
    /**
     * Scan stop row
     */
    public static final String SCAN_ROW_STOP = "hbase.mapreduce.scan.row.stop";
    /**
     * Column Family to Scan
     */
    public static final String SCAN_COLUMN_FAMILY = "hbase.mapreduce.scan.column.family";
    /**
     * Space delimited list of columns and column families to scan.
     */
    public static final String SCAN_COLUMNS = "hbase.mapreduce.scan.columns";
    /**
     * The timestamp used to filter columns with a specific timestamp.
     */
    public static final String SCAN_TIMESTAMP = "hbase.mapreduce.scan.timestamp";
    /**
     * The starting timestamp used to filter columns with a specific range of versions.
     */
    public static final String SCAN_TIMERANGE_START = "hbase.mapreduce.scan.timerange.start";
    /**
     * The ending timestamp used to filter columns with a specific range of versions.
     */
    public static final String SCAN_TIMERANGE_END = "hbase.mapreduce.scan.timerange.end";
    /**
     * The maximum number of version to return.
     */
    public static final String SCAN_MAXVERSIONS = "hbase.mapreduce.scan.maxversions";
    /**
     * Set to false to disable server-side caching of blocks for this scan.
     */
    public static final String SCAN_CACHEBLOCKS = "hbase.mapreduce.scan.cacheblocks";
    /**
     * The number of rows for caching that will be passed to scanners.
     */
    public static final String SCAN_CACHEDROWS = "hbase.mapreduce.scan.cachedrows";
    /**
     * Set the maximum number of values to return for each call to next().
     */
    public static final String SCAN_BATCHSIZE = "hbase.mapreduce.scan.batchsize";
    /**
     * Specify if we have to shuffle the map tasks.
     */
    public static final String SHUFFLE_MAPS = "hbase.mapreduce.inputtable.shufflemaps";

    /**
     * The configuration.
     */
    private Configuration conf = null;

    /**
     * Returns the current configuration.
     *
     * @return The current configuration.
     * @see org.apache.hadoop.conf.Configurable#getConf()
     */
    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * Sets the configuration. This is used to set the details for the table to
     * be scanned.
     *
     * @param configuration  The configuration to set.
     * @see org.apache.hadoop.conf.Configurable#setConf(
     *org.apache.hadoop.conf.Configuration)
     */

    /**
     * Converts the given Base64 string back into a Scan instance.
     *
     * @param base64 The scan details.
     * @return The newly created Scan instance.
     * @throws IOException When reading the scan instance fails.
     */
    static Scan convertStringToScan(String base64) throws IOException {
        byte[] decoded = Base64.decode(base64);
        ClientProtos.Scan scan;
        try {
            scan = ClientProtos.Scan.parseFrom(decoded);
        } catch (InvalidProtocolBufferException ipbe) {
            throw new IOException(ipbe);
        }

        return ProtobufUtil.toScan(scan);
    }


    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;

        Scan scan = null;

        if (conf.get(SCAN) != null) {
            try {
                scan = convertStringToScan(conf.get(SCAN));
            } catch (IOException e) {
                LOG.error("An error occurred.", e);
            }
        } else {
            try {
                scan = new Scan();

                if (conf.get(SCAN_ROW_START) != null) {
                    scan.setStartRow(Bytes.toBytes(conf.get(SCAN_ROW_START)));
                }

                if (conf.get(SCAN_ROW_STOP) != null) {
                    scan.setStopRow(Bytes.toBytes(conf.get(SCAN_ROW_STOP)));
                }

                if (conf.get(SCAN_COLUMNS) != null) {
                    addColumns(scan, conf.get(SCAN_COLUMNS));
                }

                if (conf.get(SCAN_COLUMN_FAMILY) != null) {
                    scan.addFamily(Bytes.toBytes(conf.get(SCAN_COLUMN_FAMILY)));
                }

                if (conf.get(SCAN_TIMESTAMP) != null) {
                    scan.setTimeStamp(Long.parseLong(conf.get(SCAN_TIMESTAMP)));
                }

                if (conf.get(SCAN_TIMERANGE_START) != null && conf.get(SCAN_TIMERANGE_END) != null) {
                    scan.setTimeRange(
                            Long.parseLong(conf.get(SCAN_TIMERANGE_START)),
                            Long.parseLong(conf.get(SCAN_TIMERANGE_END)));
                }

                if (conf.get(SCAN_MAXVERSIONS) != null) {
                    scan.setMaxVersions(Integer.parseInt(conf.get(SCAN_MAXVERSIONS)));
                }

                if (conf.get(SCAN_CACHEDROWS) != null) {
                    scan.setCaching(Integer.parseInt(conf.get(SCAN_CACHEDROWS)));
                }

                if (conf.get(SCAN_BATCHSIZE) != null) {
                    scan.setBatch(Integer.parseInt(conf.get(SCAN_BATCHSIZE)));
                }

                // false by default, full table scans generate too much BC churn
                scan.setCacheBlocks((conf.getBoolean(SCAN_CACHEBLOCKS, false)));
            } catch (Exception e) {
                LOG.error(StringUtils.stringifyException(e));
            }
        }

        setScan(scan);
    }

    @Override
    protected void initialize(JobContext context) throws IOException {
        // Do we have to worry about mis-matches between the Configuration from setConf and the one
        // in this context?
        TableName tableName = TableName.valueOf(conf.get(INPUT_TABLE));
        try {
            initializeTable(ConnectionFactory.createConnection(new Configuration(conf)), tableName);
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }
    }

    /**
     * Parses a combined family and qualifier and adds either both or just the
     * family in case there is no qualifier. This assumes the older colon
     * divided notation, e.g. "family:qualifier".
     *
     * @param scan               The Scan to update.
     * @param familyAndQualifier family and qualifier
     * @throws IllegalArgumentException When familyAndQualifier is invalid.
     */
    private static void addColumn(Scan scan, byte[] familyAndQualifier) {
        byte[][] fq = KeyValue.parseColumn(familyAndQualifier);
        if (fq.length == 1) {
            scan.addFamily(fq[0]);
        } else if (fq.length == 2) {
            scan.addColumn(fq[0], fq[1]);
        } else {
            throw new IllegalArgumentException("Invalid familyAndQualifier provided.");
        }
    }

    /**
     * Adds an array of columns specified using old format, family:qualifier.
     * <p>
     * Overrides previous calls to {@link Scan#addColumn(byte[], byte[])}for any families in the
     * input.
     *
     * @param scan    The Scan to update.
     * @param columns array of columns, formatted as <code>family:qualifier</code>
     * @see Scan#addColumn(byte[], byte[])
     */
    public static void addColumns(Scan scan, byte[][] columns) {
        for (byte[] column : columns) {
            addColumn(scan, column);
        }
    }

    /**
     * Calculates the splits that will serve as input for the map tasks. The
     * number of splits matches the number of regions in a table. Splits are shuffled if
     * required.
     *
     * @param context The current job context.
     * @return The list of input splits.
     * @throws IOException When creating the list of splits fails.
     * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
     *org.apache.hadoop.mapreduce.JobContext)
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> splits = super.getSplits(context);
        if ((conf.get(SHUFFLE_MAPS) != null) && "true".equals(conf.get(SHUFFLE_MAPS).toLowerCase())) {
            Collections.shuffle(splits);
        }
        //把 split 分开 ， 500*16 = 80000 个
        // 如果开始 和结束的  都是没有分裂的 分区，一拆四
        List<InputSplit> newSplits = new ArrayList<InputSplit>();
        for (int i = 0; i < splits.size(); i++) {
            TableSplit split = (TableSplit) splits.get(i);
            String startRow = new String(split.getStartRow());
            String stopRow = new String(split.getEndRow());
            if (!startRow.trim().isEmpty() && !stopRow.trim().isEmpty()  && startRow.trim().length()>=4) {
                String startPre = startRow.trim().substring(0,4);
                List<InputSplit> generatorSplit = tableSplitGs(split,startPre);
                for(int j = 0 ;j<generatorSplit.size();j++){
                    TableSplit newsplit = (TableSplit) generatorSplit.get(j);
                    String newStartRow = new String(newsplit.getStartRow());
                    String newStopRow = new String(newsplit.getEndRow());
                    if(!"491-e".equals(newStartRow)){
                    // 这里 即使分裂了，有可能 有008-ab ...008-ac  这样的分区吗，即使有，也不可能有两个一样的。所以下面的逻辑是没有问题的。
                    if(newStartRow.compareTo(startRow)>=0  && newStopRow.compareTo(stopRow) <=0 )
                        newSplits.add(new TableSplit(newsplit.getTable(),
                                Bytes.toBytes(newStartRow), Bytes.toBytes(newStopRow), newsplit.getRegionLocation(), 0));
                    else if(newStartRow.compareTo(startRow)<0  && newStopRow.compareTo(startRow)>=0  && newStopRow.compareTo(stopRow) <=0)
                        newSplits.add(new TableSplit(newsplit.getTable(),
                                Bytes.toBytes(startRow), Bytes.toBytes(newStopRow), newsplit.getRegionLocation(), 0));
                    else if(newStartRow.compareTo(startRow)>=0 && newStartRow.compareTo(stopRow)<0   && newStopRow.compareTo(stopRow) >0)
                        newSplits.add(new TableSplit(newsplit.getTable(),
                                Bytes.toBytes(newStartRow), Bytes.toBytes(stopRow), newsplit.getRegionLocation(), 0));
                    else if(newStartRow.compareTo(startRow)<0  && newStopRow.compareTo(stopRow) >0)
                        newSplits.add(new TableSplit(newsplit.getTable(),
                                Bytes.toBytes(startRow), Bytes.toBytes(stopRow), newsplit.getRegionLocation(), 0));
                    }
                }
            } else {
                newSplits.add(split);
            }
        }
        for (int i = 0; i < newSplits.size(); i++) System.out.println("tablesplit:" + newSplits.get(i).toString());
        return newSplits;
    }


    private static List<InputSplit> tableSplitGs(TableSplit input, String startpre) {
        List<InputSplit> newSplits = new ArrayList<InputSplit>();
        newSplits.add(tableSplitG(input, startpre,"0", "1"));
        newSplits.add(tableSplitG(input, startpre,"1", "2"));
        newSplits.add(tableSplitG(input, startpre,"2", "3"));
        newSplits.add(tableSplitG(input, startpre,"3", "4"));
        newSplits.add(tableSplitG(input, startpre,"4", "5"));
        newSplits.add(tableSplitG(input, startpre,"5", "6"));
        newSplits.add(tableSplitG(input, startpre,"6", "7"));
        newSplits.add(tableSplitG(input, startpre,"7", "8"));
        newSplits.add(tableSplitG(input, startpre,"8", "9"));
        newSplits.add(tableSplitG(input, startpre,"9", "a"));
        newSplits.add(tableSplitG(input, startpre,"a", "b"));
        newSplits.add(tableSplitG(input, startpre,"b", "c"));
        newSplits.add(tableSplitG(input, startpre,"c", "d"));
        newSplits.add(tableSplitG(input, startpre,"d", "e"));
        newSplits.add(tableSplitG(input, startpre,"e", "f"));
        newSplits.add(tableSplitG(input, startpre,"f", "~"));
        return newSplits;

    }




    private static TableSplit tableSplitG(TableSplit input, String startpre, String start, String end) {
        TableSplit split = new TableSplit(input.getTable(),
                    Bytes.toBytes(startpre + start), Bytes.toBytes(startpre + end), input.getRegionLocation(), 0);
        return split;
    }

    /**
     * Convenience method to parse a string representation of an array of column specifiers.
     *
     * @param scan    The Scan to update.
     * @param columns The columns to parse.
     */
    private static void addColumns(Scan scan, String columns) {
        String[] cols = columns.split(" ");
        for (String col : cols) {
            addColumn(scan, Bytes.toBytes(col));
        }
    }

    @Override
    protected Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        if (conf.get(SPLIT_TABLE) != null) {
            TableName splitTableName = TableName.valueOf(conf.get(SPLIT_TABLE));
            try (Connection conn = ConnectionFactory.createConnection(getConf())) {
                try (RegionLocator rl = conn.getRegionLocator(splitTableName)) {
                    return rl.getStartEndKeys();
                }
            }
        }
        return super.getStartEndKeys();
    }

    /**
     * Sets split table in map-reduce job.
     */
    public static void configureSplitTable(Job job, TableName tableName) {
        job.getConfiguration().set(SPLIT_TABLE, tableName.getNameAsString());
    }
}
