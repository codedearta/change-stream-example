import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkTest {

    private MongoClient mongoClient;
    private String dbName  = "DWH_TEST";
    private String landingCollectionName = "mongo_landing";
    private String bucketCollectionName = "mongo_buckets";
    private int bucketSize = 100;
    private String amountFieldName = "trx_orig_amt";
    private String currencyFieldName = "settl_curr_code";
    private String cardTypeFieldName = "card_schemes";
    private String orgNumberFieldName = "settl_card_acc_org_num";
    private String batchRefNumFieldName = "batch_ref_num";
    private int sequences =10;
    private int parallelFunctionCalls = 2;
    private int secondsRange =3;
    private String organisationNumberFieldName = "settl_card_acc_org_num";
    private String agreementIdFieldName = "agreement_id";
    private String organistaionsCollectionName = "mongo_organisations";

    public SparkTest() throws NoSuchAlgorithmException {
        var connectionString = "mongodb://localhost/";
        mongoClient = MongoClients.create(connectionString);
    }

    public class Payment {
        private LocalDateTime purchase_date;
        private String transaction_id;
        private String agreement_id;
        private String settl_curr_code;
        private java.math.BigDecimal trx_orig_amt;
        private String card_schemes;
        private String settl_card_acc_org_num;
        private String batch_ref_num;

        public LocalDateTime getPurchase_date() {
            return purchase_date;
        }

        public String getTransaction_id() {
            return transaction_id;
        }

        public String getAgreement_id() {
            return agreement_id;
        }

        public String getSettl_curr_code() {
            return settl_curr_code;
        }

        public BigDecimal getTrx_orig_amt() {
            return trx_orig_amt;
        }

        public String getCard_schemes() {
            return card_schemes;
        }

        public String getSettl_card_acc_org_num() {
            return settl_card_acc_org_num;
        }

        public String getBatch_ref_num() {
            return batch_ref_num;
        }

        public Payment(LocalDateTime purchase_date, String transaction_id, String agreement_id, String settl_curr_code, BigDecimal trx_orig_amt, String card_schemes, String settl_card_acc_org_num, String batch_ref_num) {
            this.purchase_date = purchase_date;
            this.transaction_id = transaction_id;
            this.agreement_id = agreement_id;
            this.settl_curr_code = settl_curr_code;
            this.trx_orig_amt = trx_orig_amt;
            this.card_schemes = card_schemes;
            this.settl_card_acc_org_num = settl_card_acc_org_num;
            this.batch_ref_num = batch_ref_num;
        }
    }

    @Test
    public void transform() throws InterruptedException {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        final MongoCollection<Document>landingCollection = db.getCollection(landingCollectionName);
        final MongoCollection<Document>bucketCollection = db.getCollection(bucketCollectionName);
        final MongoCollection<Document>organisationsCollection = db.getCollection(organistaionsCollectionName);

        List<Payment> paymentsList = new ArrayList<>();

        paymentsList.add(new Payment(
                LocalDateTime.parse("2018-01-02T00:00:00"),
                "1",
                "1",
                "DKK",
                new BigDecimal(12),
                "VISA",
                "1",
                "1"
                ));

        paymentsList.add(new Payment(
                LocalDateTime.parse("2018-01-01T00:00:00"),
                "2",
                "1",
                "DKK",
                new BigDecimal(100),
                "VISA",
                "1",
                "1"
        ));


        SparkConf sparkConf = new SparkConf().setAppName("spark_first").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Payment> paymentsRDD = sparkContext.parallelize(paymentsList);


        paymentsRDD.foreach(p -> {
            // apply bucket upsert logic here.
        });
        sparkContext.close();
    }

        //bulkFor(landingCollection, bucketCollection, LocalDateTime.parse("2018-01-01T00:00:00"), LocalDateTime.parse("2018-01-01T00:00:01"));
       // ExecutorService EXEC = Executors.newCachedThreadPool();

//
//        for(int sequence=1;sequence<=sequences;sequence++) {
//            Range nextRange = giveNextRange(parallelFunctionCalls*secondsRange);
//            LocalDateTime start = nextRange.start;
//            LocalDateTime end = start.plusSeconds(secondsRange);
//
//            List<Callable<BulkWriteResult>> tasks = new ArrayList<Callable<BulkWriteResult>>();
//            for(int i=0; i<parallelFunctionCalls;i++) {
//                System.out.println("add to processing start:"+start+" end:"+end);
//                LocalDateTime finalStart = start;
//                LocalDateTime finalEnd = end;
//                tasks.add(() -> bulkFor(landingCollection, bucketCollection, organisationsCollection, finalStart, finalEnd));
//                start = start.plusSeconds(secondsRange);
//                end = end.plusSeconds(secondsRange);
//            }
//
//            List<Future<BulkWriteResult>> futures = EXEC.invokeAll(tasks);
//            futures.forEach(f -> { System.out.println(f.toString()); });
//
//        }

}
