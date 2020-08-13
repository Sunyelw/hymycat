package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;

/**
 * 项目名称:   pinkstone
 * 包:        io.mycat.route.function
 * 类名称:     PartitionByHashAndMonthSubString
 * 类描述:     1
 * 创建人:     huangyang
 * 创建时间:   2020/8/13 17:35
 */
public class PartitionByHashAndMonthSubString extends AbstractPartitionAlgorithm implements RuleAlgorithm {

    @Override
    public Integer calculate(String columnValue){
        return null;
    }
}
