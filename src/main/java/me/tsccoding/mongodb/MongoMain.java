package me.tsccoding.mongodb;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class MongoMain
{
	private static final String DATABASE_NAME = "myMongoDb";
	private static final String CUSTOMERS_COLLECTION_NAME = "customers";

	public static void main(String[] args)
	{
		MongoClient mongoClient = getMongoConnection();

		//get list all DB
		MongoIterable<String> listDatabaseNames = mongoClient.listDatabaseNames();
		printMongoIterableCollection(listDatabaseNames);

		//create collection (uncomment code below if need to create DB)
		//createCollectionForDB(mongoClient);

		//insert into DB (uncomment if need to insert new one)
		//insertIntoCollection(mongoClient);

		//read document from collection
        readCollectionFromDB(mongoClient);

        //update document from collection
        //updateDocumentInCollection(mongoClient);

        //read document from collection
        readCollectionFromDB(mongoClient);

        deleteDocumentInCollection(mongoClient);

        //read document from collection
        readCollectionFromDB(mongoClient);
	}

    private static void readCollectionFromDB(MongoClient mongoClient)
    {
        MongoDatabase myMongoDb = mongoClient.getDatabase(DATABASE_NAME);
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("name", "John");
        MongoCollection<Document> collection = myMongoDb.getCollection(CUSTOMERS_COLLECTION_NAME);
        //FindIterable<Document> cursor = collection.find(searchQuery);
        FindIterable<Document> cursor = collection.find();
        MongoCursor<Document> iterator = cursor.iterator();
        while (iterator.hasNext())
        {
            System.out.println(iterator.next());
        }
    }

	private static void createCollection(MongoClient mongoClient)
	{
		MongoDatabase myMongoDb = mongoClient.getDatabase(DATABASE_NAME);
		myMongoDb.createCollection(CUSTOMERS_COLLECTION_NAME);
		MongoIterable<String> listCollectionNames = myMongoDb.listCollectionNames();
		printMongoIterableCollection(listCollectionNames);
	}

	private static void insertIntoCollection(MongoClient mongoClient)
	{
		MongoDatabase myMongoDb = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = myMongoDb.getCollection(CUSTOMERS_COLLECTION_NAME);
        Document document = new Document();
        document.put("name", "Max");
        document.put("company", "Microsoft");
        collection.insertOne(document);
	}

    private static void updateDocumentInCollection(MongoClient mongoClient)
    {
        MongoDatabase myMongoDb = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = myMongoDb.getCollection(CUSTOMERS_COLLECTION_NAME);
        Document oldDocument = new Document();
        oldDocument.put("name", "Max");

        Document newDocument = new Document();
        newDocument.put("name", "John");

        Document updateDocument = new Document();
        updateDocument.put("$set", newDocument);

        collection.updateOne(oldDocument, updateDocument);
    }

    private static void deleteDocumentInCollection(MongoClient mongoClient)
    {
        MongoDatabase myMongoDb = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = myMongoDb.getCollection(CUSTOMERS_COLLECTION_NAME);
        Document document = new Document();
        document.put("name", "John");
        collection.deleteOne(document);
    }

	private static void printMongoIterableCollection(MongoIterable<String> strings)
	{
		MongoCursor<String> iterator = strings.iterator();
		while (iterator.hasNext())
		{
			System.out.println(iterator.next());
		}
	}

	private static MongoClient getMongoConnection()
	{
		return new MongoClient("localhost", 27017);
	}
}
