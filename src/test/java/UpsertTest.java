import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled
public class UpsertTest {

    private MongoClient mongoClient;
    private String dbName  = "rbs";
    private String collectionName = "pds";
    private MessageDigest m;

    public UpsertTest() throws NoSuchAlgorithmException {
        m = MessageDigest.getInstance("MD5");
        var connectionString = "mongodb://localhost/?w=majority";
        mongoClient = MongoClients.create(connectionString);
    }

    @BeforeAll
    @Disabled
    public void initDb() {
        try {
            mongoClient.getDatabase(dbName).drop();
            mongoClient.getDatabase("admin").runCommand(new Document("enableSharding", "rbs"));
            mongoClient.getDatabase("admin").runCommand(new Document()
                    .append("shardCollection", "rbs.pds")
                    .append("key", new Document()
                            .append("hashIdMod", 1)
                            .append("_id", 1)
                            .append("activeInd", 1)
                    )
            );

            // set the chunk size to 1MB for testing purpose
            mongoClient.getDatabase("config")
                    .getCollection("settings")
                    .updateOne(
                            new Document("_id", "chunksize"),
                            new Document("$set", new Document("value", 1)),
                            new UpdateOptions().upsert(true)
                    );
        }
        catch(Exception e1) {
                e1.printStackTrace();
            }
    }


    @Test
    @Disabled
    public void upsertCreated() {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        final MongoCollection<Document>collection = db.getCollection(collectionName);

        try {
            // e2eId components
            long prefix = 2412201915771792906L;
            long postfix = 617703905093645629L;
            long upperBound = 617703905093645629L + 20;

            for(long i=postfix;i<upperBound;i++) {
                // query document
                String etoeid = Long.toString(prefix) + "." + Long.toString(i);
                short hashIdMod = getHashIdMod(etoeid,720);

                Document query = new Document();
                query.append("hashIdMod", hashIdMod);
                query.append("_id", etoeid);
                query.append("activeInd", "ACTIVE");

                // update document
                Date clientDate = new Date();

                Document update = new Document();
                update.append("$setOnInsert", new Document("created", clientDate));
                byte[] bytes = new byte[1024*100]; // add a 100kb byte array to each document to increase document size and force chunk splitting
                update.append("$set", new Document("updated", clientDate).append("bytes", bytes));

                // upsert
                collection.updateOne(query, update, new UpdateOptions().upsert(true));
            }
        } catch (NoSuchAlgorithmException e1) {
            e1.printStackTrace();
        }
    }

    private short getHashIdMod(String etoeid, int modulo) throws NoSuchAlgorithmException {
        m.reset();
        m.update(etoeid.getBytes());
        byte[] digest = m.digest();
        BigInteger hashIdModulo = new BigInteger(1, digest);
        short hashIdMod = hashIdModulo.mod(new BigInteger(String.valueOf(modulo))).shortValue();
        return hashIdMod;
    }
}
