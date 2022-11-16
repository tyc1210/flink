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
 * @date 2022-11-16 15:09:54
 */
public class FraudDetector3 extends KeyedProcessFunction<Long, Transaction, Alert> {private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    //前一次交易小于$1的状态标识
    private transient ValueState<Boolean> flagState;

    //前一次交易小于$1时，注册定时器
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timeDescriptor = new ValueStateDescriptor<>("time-state", Types.LONG);
        timerState = getRuntimeContext().getState(timeDescriptor);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        //获取上一次计算的状态
        Boolean value = flagState.value();
        //上一次计算的状态非空，说明上一次状态<$1
        if(Objects.nonNull(value)) {
            //如果当前金额>$500，则报警
            if(transaction.getAmount()>LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            cleanUp(context);
        }

        //判断当前状态是否<$1，小于则标记并更新状态
        if(transaction.getAmount()<SMALL_AMOUNT) {
            flagState.update(true);
            /**
             * 设置一个当前时间一分钟后触发的定时器
             * 同时，将触发时间保存到 timerState 状态中。
             * 当定时器触发时，将会调用 KeyedProcessFunction#onTimer 方法
             */
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // clean up all state
        timerState.clear();
        flagState.clear();
    }


}
