import { MongoClient, Db } from "mongodb";

const MONGODB_URI = process.env.MONGODB_URI || "mongodb://localhost:27017";
const DB_NAME = "performance_test";

let client: MongoClient | null;
let db: Db | null;

// Connect to MongoDB
export async function connectToMongoDB(): Promise<Db> {
    if (db) {
        return db;
    }

    client = new MongoClient(MONGODB_URI);
    await client.connect();
    db = client.db(DB_NAME);
    await db.createIndex("documents", { user: 1, _id: 1 });
    console.log("Connected to MongoDB");
    return db;
}

// Close MongoDB connection
export async function closeMongoDB(): Promise<void> {
    if (client) {
        await client.close();
        client = null;
        db = null;
    }
}
