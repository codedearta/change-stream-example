import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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
public class BucketTest {

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

    public BucketTest() throws NoSuchAlgorithmException {
        var connectionString = "mongodb://localhost/";
        mongoClient = MongoClients.create(connectionString);
    }

    @Test
    public void transform() throws InterruptedException {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        final MongoCollection<Document>landingCollection = db.getCollection(landingCollectionName);
        final MongoCollection<Document>bucketCollection = db.getCollection(bucketCollectionName);
        final MongoCollection<Document>organisationsCollection = db.getCollection(organistaionsCollectionName);


        //bulkFor(landingCollection, bucketCollection, LocalDateTime.parse("2018-01-01T00:00:00"), LocalDateTime.parse("2018-01-01T00:00:01"));
        ExecutorService EXEC = Executors.newCachedThreadPool();


        for(int sequence=1;sequence<=sequences;sequence++) {
            Range nextRange = giveNextRange(parallelFunctionCalls*secondsRange);
            LocalDateTime start = nextRange.start;
            LocalDateTime end = start.plusSeconds(secondsRange);

            List<Callable<BulkWriteResult>> tasks = new ArrayList<Callable<BulkWriteResult>>();
            for(int i=0; i<parallelFunctionCalls;i++) {
                System.out.println("add to processing start:"+start+" end:"+end);
                LocalDateTime finalStart = start;
                LocalDateTime finalEnd = end;
                tasks.add(() -> bulkFor(landingCollection, bucketCollection, organisationsCollection, finalStart, finalEnd));
                start = start.plusSeconds(secondsRange);
                end = end.plusSeconds(secondsRange);
            }

            List<Future<BulkWriteResult>> futures = EXEC.invokeAll(tasks);
            futures.forEach(f -> { System.out.println(f.toString()); });

        }
    }

    private BulkWriteResult bulkFor(MongoCollection<Document> landingCollection, MongoCollection<Document> bucketCollection, MongoCollection<Document> organisationsCollection, LocalDateTime from, LocalDateTime to) {

        Document filter = getRangeFilter(from, to);

        List docs = new ArrayList();
        List orgUpdates = new ArrayList();

        landingCollection.find(filter).forEach(doc -> {
            Document searchId = searchIdFor(doc);
            Document updateOne = bucketUpdateFor(doc);
            docs.add(new UpdateOneModel<>(searchId, updateOne, new UpdateOptions().upsert(true)));

            Document searchIdOr = new Document("_id", doc.get(organisationNumberFieldName));
            Document updateOneOrg = new Document("$addToSet", new Document("agreements",doc.get(agreementIdFieldName)));
            orgUpdates.add(new UpdateOneModel<>(searchIdOr, updateOneOrg, new UpdateOptions().upsert(true)));
        });

        organisationsCollection.bulkWrite(orgUpdates,new BulkWriteOptions().ordered(false));
        return bucketCollection.bulkWrite(docs,new BulkWriteOptions().ordered(false));

    }



    private Document getRangeFilter(LocalDateTime from, LocalDateTime to) {
        Document range = new Document();
        range.append("$gte", from);
        range.append("$lt", to);
        Document filter = new Document();
        filter.append("purchase_date", range);
//        filter.append("agreement_id", "20209540557_17204499");
        return filter;
    }

    private Document bucketUpdateFor(Document doc) {
        Document bucketUpdateObejct = new Document();
        bucketUpdateObejct.append("$push", new Document("transactions", doc));
        bucketUpdateObejct.append("$inc", new Document("count", 1));
        bucketUpdateObejct.append("$max", new Document( "maxAmount", doc.get(amountFieldName)));
        bucketUpdateObejct.append("$min", new Document( "minAmount", doc.get(amountFieldName)));

        Document addToSet = new Document();
        addToSet.append("currencies", doc.get(currencyFieldName));
        addToSet.append("cardTypes", doc.get(cardTypeFieldName));
        addToSet.append("batchRefNumbers", doc.get(batchRefNumFieldName));
        bucketUpdateObejct.append("$addToSet", addToSet);

        bucketUpdateObejct.append("$set", new Document("org_num", doc.get(orgNumberFieldName)));

        return bucketUpdateObejct;
    }

    private Document searchIdFor(Document doc) {
        Document searchId = new Document("agreement_id", doc.getString("agreement_id"));
        searchId.append("day", toDayInteger(doc));
        searchId.append("count", new Document("$lt", bucketSize));
        return searchId;
    }

    private int toDayInteger(Document doc) {
        LocalDateTime purchase_date = toLocalDateTime((doc.getDate("purchase_date")));
        return purchase_date.getYear() *10000 + (purchase_date.getMonthValue() +1) * 100 + purchase_date.getDayOfMonth();
    }

    private LocalDateTime toLocalDateTime(Date dateToConvert) {
        return Instant.ofEpochMilli(dateToConvert.getTime())
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
    }

    private LocalDateTime lastEnd = LocalDateTime.parse("2018-01-01T00:00:00");
    private synchronized Range giveNextRange(int seconds) {
        Range ret = new Range();
        ret.start = lastEnd;
        ret.end = lastEnd.plusSeconds(seconds);
        lastEnd = ret.end;
        System.out.println("next range: seconds:"+seconds+" start:"+ret.start+" end:"+ret.end);
        return ret;
    }

    private class Range {
        public LocalDateTime start;
        public LocalDateTime end;
    }
}
