package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.Properties;

/**
 * 按月分片 按天分表
 */
public class PartitionByHashAndMonthSubString extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private String mapFile;
    private SubStringRange[] stringRanges;
    private int subLength = 0;
    private final int MAX_NUMS = 10000;

    private int defaultNode = -1;

    public void init() {
        initialize();
    }

    public void setMapFile(String mapFile) {
        this.mapFile = mapFile;
    }

    public Integer calculate(String columnValue) {
        try {
            Integer rst = null;
            for (SubStringRange stringRang : this.stringRanges) {
                if (columnValue.substring(0, this.subLength).equals(stringRang.value)) {
                    return stringRang.groupSize + Integer.parseInt(columnValue.substring(this.subLength)) - 1;
                }
            }

            if (this.defaultNode >= 0) {
                return this.defaultNode;
            }
            return rst;
        }
        catch (NumberFormatException ignored) {
        }

        throw new IllegalArgumentException("columnValue:" + columnValue
                + " Please eliminate any quote and non number within it.");
    }

    public int getPartitionNum() {
        int nPartition = 0;
        for (PartitionByHashAndMonthSubString.SubStringRange longRange : this.stringRanges) {
            nPartition += longRange.groupSize;
        }
        return nPartition;
    }

    private void initialize()
    {
        InputStream fin = null;
        try {
            fin = getClass().getClassLoader()
                    .getResourceAsStream(this.mapFile);
            if (fin == null) {
                throw new RuntimeException("can't find class resource file " +
                        this.mapFile);
            }
            Properties prop = new Properties();
            prop.load(fin);
            String subLength = prop.getProperty("sub-length");
            if ((subLength == null) || (subLength.trim().length() == 0)) {
                throw new RuntimeException(this.mapFile + "->key:sub-length error!");
            }

            this.subLength = Integer.parseInt(subLength);

            String subList = prop.getProperty("sub-list");
            if ((subList == null) || (subList.trim().length() == 0)) {
                throw new RuntimeException(this.mapFile + "->key:sub-list error!");
            }
            LinkedList<SubStringRange> stringRangeList = new LinkedList <>();
            String[] subs = subList.split(",");
            for (String sub : subs) {
                int nodeId = Integer.parseInt(prop.getProperty(sub).trim());
                stringRangeList.add(new SubStringRange(nodeId, sub));
            }
            this.stringRanges = stringRangeList.toArray(new SubStringRange[0]);
        }
        catch (Exception e) {
            if ((e instanceof RuntimeException)) {
                throw ((RuntimeException)e);
            }
            throw new RuntimeException(e);
        } finally {
            try {
                assert fin != null;
                fin.close();
            } catch (Exception ignored) {
            }
        }
    }

    public int getDefaultNode() {
        return this.defaultNode;
    }

    public void setDefaultNode(int defaultNode) {
        this.defaultNode = defaultNode;
    }

    static class SubStringRange
    {
        final int groupSize;
        final String value;

        public SubStringRange(int groupSize, String value)
        {
            this.groupSize = groupSize;
            this.value = value;
        }
    }
}