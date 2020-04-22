import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ChangeStreamsTest {

    private MongoClient mongoClient;
    private String dbName  = "configuration";
    private String mappingCollectionName = "mapping";
    private String mappingNewCollectionName = "mappingNew";
    private String mappingMongoCollectionName = "mappingMongo";
    private Gson gson = new GsonBuilder().setPrettyPrinting().setLenient().create();

    public ChangeStreamsTest(){
        var connectionString = "mongodb://localhost/?w=majority";
        mongoClient = MongoClients.create(connectionString);
    }

    @BeforeAll
    public void initDb() throws IOException {
        mongoClient.getDatabase(dbName).drop();
        initMappingMongo();
        initMapping();
        initMappingNew();
    }

    private void initMappingMongo() {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection<Document> mappingMongoCollection = db.getCollection(mappingMongoCollectionName);

        mappingMongoCollection.insertOne(
                new Document("_id", "transferInitiationReceived.customerPaymentStatusReport.grpHdr.msgId")
                .append("type", "transferInitiationReceived")
                .append("value", 12345)
        );

        mappingMongoCollection.insertOne(
                new Document("_id", "transferInitiationReceived.customerPaymentStatusReport.grpHdr.creDtTm")
                .append("type", "transferInitiationReceived")
                .append("value", new Date())
        );

        mappingMongoCollection.insertOne(
                new Document("_id", "transferInitiationReceived.customerPaymentStatusReport.grpHdr.nbOfTxs")
                .append("type", "transferInitiationReceived")
                .append("value", 5)
        );

        mappingMongoCollection.insertOne(
                new Document("_id", "pens.customerPaymentStatusReport.grpHdr.creDtTm")
                        .append("type", "pens")
                        .append("value", new Date())
        );

        mappingMongoCollection.insertOne(
                new Document("_id", "pens.customerPaymentStatusReport.grpHdr.nbOfTxs")
                        .append("type", "pens")
                        .append("value", 5)
        );
    }

    private void initMapping() throws IOException {
        MongoDatabase db = mongoClient.getDatabase(dbName);

        db.getCollection(mappingCollectionName).drop();

        ObjectMapper objectMapper = new ObjectMapper();
        ClassLoader classLoader = getClass().getClassLoader();

        MongoCollection<Document> mappingCollection = db.getCollection(mappingCollectionName);

        File mappingFile = new File(classLoader.getResource("eventname_jsonfield_mapping.json").getFile());
        HashMap<String,Object>[] mapping = objectMapper.readValue(mappingFile, HashMap[].class);
        Arrays.stream(mapping).forEach(doc -> mappingCollection.insertOne(new Document(doc)));
    }

    private void initMappingNew() throws IOException {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        db.getCollection(mappingNewCollectionName).drop();

        ObjectMapper objectMapper = new ObjectMapper();
        ClassLoader classLoader = getClass().getClassLoader();

        MongoCollection<Document> mappingNewCollection = db.getCollection(mappingNewCollectionName);
        File mappingNewFile = new File(classLoader.getResource("eventname_jsonfield_mappingnew.json").getFile());
        HashMap<String,Object> mappingNew = objectMapper.readValue(mappingNewFile, HashMap.class);
        mappingNewCollection.insertOne( new Document(mappingNew));
    }



    // db.mapping.updateOne({ type: "TInitiationReceived" },{ $set: { "mappings.customerPaymentStatusReport_grpHdr_msgId": 4  }})
    @Test
    public void trackChangesMappingTest() {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        final MongoCollection<Document> collection = db.getCollection(mappingCollectionName);

        final Bson match = Aggregates.match(Document.parse("{'fullDocument.type': 'transferInitiationReceived'}"));

        final List<Bson> pipeline = asList(match);

        assertThrows(RuntimeException.class, () -> {
            collection
                    .watch(pipeline)
                    .fullDocument(FullDocument.UPDATE_LOOKUP)
                    .forEach(csEvent -> {
                        // throw an exception to break the loop and report a change
                        System.out.println(gson.toJson(csEvent));
                        throw new RuntimeException(gson.toJson(csEvent));
                    });
        });
    }

    @Test
    @Disabled
    public void trackChangesMappingMongoTest() {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        final MongoCollection<Document> collection = db.getCollection(mappingMongoCollectionName);
        final Bson match = Aggregates.match(Document.parse("{'fullDocument.type': 'transferInitiationReceived'}"));
        final List<Bson> pipeline = asList(match);

        assertThrows(RuntimeException.class, () -> {
            collection
                    .watch(pipeline)
                    .fullDocument(FullDocument.UPDATE_LOOKUP)
                    .forEach(csEvent -> {
                        // throw an exception to break the loop and report a change
                        System.out.println(gson.toJson(csEvent));
                        throw new RuntimeException(gson.toJson(csEvent));
                    });
        });
    }

    @Test
    @Disabled
    public void trackChangesMappingNewTest() {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        final MongoCollection<Document> collection = db.getCollection(mappingNewCollectionName);

        final Bson match = Aggregates.match(Document.parse("{'fullDocument.events.key': 'transferInitiationReceived'}"));

        final List<Bson> pipeline = asList(match);

        assertThrows(RuntimeException.class, () -> {
            collection
                    .watch(pipeline)
                    .fullDocument(FullDocument.UPDATE_LOOKUP)
                    .forEach(csEvent -> {
                        // throw an exception to break the loop and report a change
                        System.out.println(gson.toJson(csEvent));
                        throw new RuntimeException(gson.toJson(csEvent));
                    });
        });
    }

    @AfterAll
    public void tearDown(){
        mongoClient.close();
    }
}
