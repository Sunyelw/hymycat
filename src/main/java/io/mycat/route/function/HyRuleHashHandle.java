package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 包:        io.mycat
 * 类名称:     HyRuleHashHandle
 * 类描述:     自定义分片规则
 * 创建人:     huangyang
 * 创建时间:   2019/7/25 15:42
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class HyRuleHashHandle extends AbstractPartitionAlgorithm implements RuleAlgorithm {

    private final static String SUB_LENGTH = "sub-length";
    private final static String SUB_LIST = "sub-list";
    private final static String SUB_SUFFIX = "sub-mod";
    private final static String SPLIT = ",";

    // 配置文件名称
    private String mapFile;
    // 分区个数
    private String count;
    // 截取字段长度
    private Integer subLength;
    // 默认节点
    private Integer defaultNode = -1;
    // 测试数据
    private String testPro;

    // 分区具体配置
    private List<SubConfig> configs = new ArrayList <>();

    private Map<Object, Integer> app2Partition;

    @Override
    public void init(){

        log.error("=======================");
        log.error("init_start...");
        log.error("=======================");

        // 配置文件转换
        doInit();

        log.error("+++++++++++++++++++++++++++++++++++++");
        log.error(toString());
    }

    // 实际处理
    @Override
    public Integer calculate(String columnValue){

        log.error("=======================");
        log.error("calculate_start...");
        log.error("=======================");

        Integer node = 0;
        int partition;
        String subString = columnValue.substring(0, subLength);
        for (SubConfig sub : configs) {
            if (sub.value.equals(subString)) {
                partition = getPartition(sub, columnValue);
                return node + partition;
            }
            node += sub.size;
        }
        return null;
    }

    /* other functions */

//    @Override
//    public Integer[] calculateRange(String beginValue, String endValue){
//        Integer begin, end;
//        begin = calculateStart(beginValue);
//        end = calculateEnd(endValue);
//
//        if ((begin == null) || (end == null)) {
//            return new Integer[0];
//        }
//
//        if (end >= begin) {
//            int len = end - begin + 1;
//            Integer[] re = new Integer[len];
//
//            for (int i = 0; i < len; i++) {
//                re[i] = begin + i;
//            }
//
//            return re;
//        }
//        return null;
//    }
//
//
//    private Integer calculateStart(String columnValue) {
//        int nodeIndex = 0;
//        for (SubConfig stringRang : this.configs) {
//            if (columnValue.substring(0, this.subLength).equals(stringRang.value)) {
//                return nodeIndex;
//            }
//
//            nodeIndex += stringRang.size;
//        }
//
//        if (this.defaultNode >= 0) {
//            return this.defaultNode;
//        }
//        return null;
//    }
//    private Integer calculateEnd(String columnValue) {
//        int nodeIndex = 0;
//        for (SubConfig stringRang : this.configs) {
//            if (columnValue.substring(0, this.subLength).equals(stringRang.value)) {
//                return nodeIndex + stringRang.size - 1;
//            }
//            nodeIndex += stringRang.size;
//        }
//
//        if (this.defaultNode >= 0) {
//            return this.defaultNode;
//        }
//        return null;
//    }
    
    @Override
    public int getPartitionNum(){
        SubConfig config = this.configs.get(0);
        return config.size * this.configs.size();
    }

    // 获取正确的分区
    private Integer getPartition(SubConfig subConfig, String columnValue) {
        int lg = (int) Math.abs(((long) columnValue.substring(subLength).hashCode() % 10000L));
        List<Integer> indexList = subConfig.indexList;
        for (int i = 0; i < indexList.size(); i++) {
            if (lg <= indexList.get(i)) {
                return i;
            }
        }
        return defaultNode;
    }

    /**
     * 初始化，读取配置文件
     *
     */
    private void doInit() {
        InputStream ins = null;
        try {
            ins = getClass().getClassLoader().getResourceAsStream(this.mapFile);
            if (ins == null) {
                throw new RuntimeException("can't find class resource file " + this.mapFile);
            }
            Properties po = new Properties();
            po.load(ins);
            // 拿到子字符串前缀长度
            this.subLength = Integer.valueOf(po.getProperty(SUB_LENGTH)); // 4
            String subList = po.getProperty(SUB_LIST);
            String[] subArr = null == subList ? null : subList.split(SPLIT);
            if (null == subArr) {
                throw new RuntimeException("sub-list is illegal -> ");
            }

            String indexes;
            List<Integer> indexList = new ArrayList <>();
            indexes = po.getProperty(SUB_SUFFIX);
            if (null == indexes) {
                throw new RuntimeException("sub-mod is illegal -> " + SUB_SUFFIX);
            }
            // 存储每个分区所需的范围
            String[] indexArr = indexes.split(SPLIT);
            for (String index : indexArr) {
                indexList.add(Integer.valueOf(index));
            }
            for (String sub : subArr) {
                this.configs.add(new SubConfig(indexArr.length, sub, indexList));
            }
        } catch (Exception e) {
            log.error("hy_error_hy_file_read_error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (null != ins) ins.close();
            } catch (IOException e) {
                log.error("hy_error_close_error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * 每个分区属性配置
     */
    static class SubConfig{
        // 分区所配节点数
        int size;
        // 分区关键字
        String value;
        // 分区节点分配规则
        List<Integer> indexList;

        SubConfig(int size, String value, List<Integer> indexList) {
            this.size = size;
            this.value = value;
            this.indexList = indexList;
        }

        @Override
        public String toString(){
            return "SubConfig{" +
                    "size=" + size +
                    ", value='" + value + '\'' +
                    ", indexList=" + indexList +
                    '}';
        }
    }

    @Override
    public String toString(){
        return "HyRuleHashHandle{" +
                "mapFile='" + mapFile + '\'' +
                ", count='" + count + '\'' +
                ", subLength=" + subLength +
                ", defaultNode=" + defaultNode +
                ", configs=" + configs +
                ", testPro=" + testPro +
                '}';
    }

    public static void main(String[] args){

        // test
        HyRuleHashHandle handle = new HyRuleHashHandle();
    }
}
