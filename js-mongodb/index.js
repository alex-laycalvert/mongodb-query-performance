import { MongoClient } from "mongodb";

/**
 * @param {string} uri
 * @param {string} dbName
 */
async function main(uri, dbName) {
    const client = new MongoClient(uri);

    const db = client.db(dbName);
}

main(process.argv[2], process.argv[3]);
