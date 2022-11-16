package spendreport;

import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * 对于一个账户，如果出现小于 $1 美元的交易后紧跟着一个大于 $500 的交易，就输出一个报警信息。
 */
public class FraudDetectionJob2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        DataStream<Transaction> transactions = env.addSource(new TransactionSource()).name("transactions");
        DataStream<Alert> alerts = transactions
                .keyBy(transaction -> transaction.getAccountId())
                .process(new FraudDetector2())
                .name("fraud-detector");
        alerts.addSink(new AlertSink()).name("send-alerts");
        env.execute("Fraud Detection");
    }
}
