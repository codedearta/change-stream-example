import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.reactivestreams.client.*;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.util.Arrays.asList;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveChangeStreamsTest {

    private MongoClient mongoClient;
    private String dbName  = "configuration";
    private String mappingCollectionName = "mapping";
    private Gson gson = new GsonBuilder().setPrettyPrinting().setLenient().create();

    public ReactiveChangeStreamsTest(){
        var connectionString = "mongodb://localhost/?w=majority";
        mongoClient = MongoClients.create(connectionString);
    }

    @BeforeAll
    public void initDb() throws IOException {
        mongoClient.getDatabase(dbName).drop();
        initMapping();
    }

    private void initMapping() throws IOException {
        MongoDatabase db = mongoClient.getDatabase(dbName);


        Flowable.fromPublisher(db.getCollection(mappingCollectionName).drop()).subscribe();

        ObjectMapper objectMapper = new ObjectMapper();
        ClassLoader classLoader = getClass().getClassLoader();

        MongoCollection<Document> mappingCollection = db.getCollection(mappingCollectionName);

        File mappingFile = new File(classLoader.getResource("eventname_jsonfield_mapping.json").getFile());
        HashMap<String,Object>[] mapping = objectMapper.readValue(mappingFile, HashMap[].class);
        Arrays.stream(mapping).forEach(doc -> Flowable.fromPublisher(mappingCollection.insertOne(new Document(doc))).subscribe());
    }

    @Test
    public void trackChangesMappingMultipleTest() {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        final MongoCollection<Document> collection = db.getCollection(mappingCollectionName);

        final Bson match = Aggregates.match(Document.parse("{'fullDocument.type': 'TInitiationReceived'}"));
        final List<Bson> pipeline = asList(match);
        final ChangeStreamPublisher<Document> watch = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
        @NonNull final Flowable<String> flowable = Flowable.fromPublisher(watch).map(c -> gson.toJson(c));
        @NonNull final Disposable subscribe = flowable.subscribe(System.out::println);
        Flowable.fromPublisher(collection.updateOne(new Document("type", "TInitiationReceived"),new Document("$set", new Document("mappings.cpsr_grpHdr_msgId", 1)))).blockingSingle();
        Flowable.fromPublisher(collection.updateOne(new Document("type", "TInitiationReceived"),new Document("$set", new Document("mappings.cpsr_grpHdr_msgId", 2)))).blockingSingle();
        Flowable.fromPublisher(collection.updateOne(new Document("type", "TInitiationReceived"),new Document("$set", new Document("mappings.cpsr_grpHdr_msgId", 3)))).blockingSingle();

        flowable.test().cancel();
    }

    @Test
    public void trackChangesMappingFirstTest() throws InterruptedException {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        final MongoCollection<Document> collection = db.getCollection(mappingCollectionName);

        final Bson match = Aggregates.match(Document.parse("{'fullDocument.type': 'TInitiationReceived'}"));
        final List<Bson> pipeline = asList(match);
        final ChangeStreamPublisher<Document> watch = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
        final @NonNull Single<Object> single = Flowable.fromPublisher(watch).firstOrError().map(c -> gson.toJson(c));
        single.subscribe(System.out::println);
        Flowable.fromPublisher(collection.updateOne(new Document("type", "TInitiationReceived"),new Document("$set", new Document("mappings.cpsr_grpHdr_msgId", 1)))).blockingSingle();
    }

    @AfterAll
    public void tearDown(){
        mongoClient.close();
    }
}
