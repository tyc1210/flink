package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.util.Objects;

/**
 * 类描述
 *
 * @author tyc
 * @version 1.0
 * @date 2022-11-16 14:47:40
 */
public class FraudDetector2 extends KeyedProcessFunction<Long, Transaction, Alert> {
    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    /**
     * 前一次交易小于$1的状态标识
     */
    private transient ValueState<Boolean> flagState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> flag = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flag);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        //获取上一次计算的状态
        Boolean value = flagState.value();
        //上一次计算的状态非空，说明上一次状态<$1
        if(Objects.nonNull(value)){
            //如果当前金额>$500，则报警
            if(transaction.getAmount()>LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            // 清空状态
            flagState.clear();
        }

        //判断当前状态是否<$1，小于则标记并更新状态
        if(transaction.getAmount()<SMALL_AMOUNT) {
            flagState.update(true);
        }
    }
}
