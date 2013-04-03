import com.mongodb.*;

import java.net.UnknownHostException;

/**
 * User: ragvena
 * Date: 2/24/13
 * Time: 11:58 AM
 */

public class MongoDBStorage {
    private static final String MONGO_STORAGE = "mailru";
    private static final String PROCESSRD = "urlsp";
    private static final String GOTED = "urlsg";
    private static MongoDBStorage instance;
    public DB database;
    public DBCollection processedCollection;
    public DBCollection finalCollection;
    public DBCollection gottedCollection;
    public DBCollection unparsedCollection;


    private MongoDBStorage() {
        Mongo mongo = null;
        try {
            mongo = new Mongo("localhost", 27017);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        database = mongo.getDB(MONGO_STORAGE);
        database.setWriteConcern(WriteConcern.NORMAL.continueOnErrorForInsert(true));
        processedCollection = database.getCollection(PROCESSRD);
        gottedCollection = database.getCollection(GOTED);
        finalCollection =database.getCollection("pages");
        unparsedCollection =database.getCollection("unparsed");
    }

    public static MongoDBStorage getInstance() {
        if (instance == null) {
            instance = new MongoDBStorage();
        }
        return instance;
    }







}
