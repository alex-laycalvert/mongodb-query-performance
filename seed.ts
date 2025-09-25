import type { Db, ObjectId } from "mongodb";

const batchSize = 10_000; // Smaller batch size for memory efficiency

// Generate random data utilities
function getRandomInt(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function getRandomString(length: number = 10): string {
    const chars =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let result = "";
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

function getRandomEmail(): string {
    const domains = [
        "gmail.com",
        "yahoo.com",
        "hotmail.com",
        "company.com",
        "test.org",
    ];
    return `${getRandomString(8)}@${domains[getRandomInt(0, domains.length - 1)]}`;
}

function getRandomArray(): any[] {
    const length = getRandomInt(1, 10);
    const arr = [];
    for (let i = 0; i < length; i++) {
        const type = getRandomInt(1, 3);
        switch (type) {
            case 1:
                arr.push(getRandomString(5));
                break;
            case 2:
                arr.push(getRandomInt(1, 1000));
                break;
            case 3:
                arr.push(Math.random() > 0.5);
                break;
        }
    }
    return arr;
}

function getRandomObject(): Record<string, any> {
    const keys = ["name", "value", "type", "status", "category", "priority"];
    const obj: Record<string, any> = {};
    const numKeys = getRandomInt(2, 4);

    for (let i = 0; i < numKeys; i++) {
        const key = keys[getRandomInt(0, keys.length - 1)];
        if (key) {
            const type = getRandomInt(1, 3);
            switch (type) {
                case 1:
                    obj[key] = getRandomString(8);
                    break;
                case 2:
                    obj[key] = getRandomInt(1, 100);
                    break;
                case 3:
                    obj[key] = Math.random() > 0.5;
                    break;
            }
        }
    }
    return obj;
}

function generateRandomProperty(): any {
    const type = getRandomInt(1, 5);
    switch (type) {
        case 1:
            return getRandomString(getRandomInt(5, 20));
        case 2:
            return getRandomInt(1, 10000);
        case 3:
            return Math.random() > 0.5;
        case 4:
            return getRandomArray();
        case 5:
            return getRandomObject();
        default:
            return getRandomString(10);
    }
}

// Generate a user document with 10-50 random properties
function generateUser(): Record<string, any> {
    const user: Record<string, any> = {
        name: `${getRandomString(6)} ${getRandomString(8)}`,
        email: getRandomEmail(),
        age: getRandomInt(18, 80),
        status: ["active", "inactive", "pending", "banned"][getRandomInt(0, 3)],
        department: ["engineering", "marketing", "sales", "support", "hr"][
            getRandomInt(0, 4)
        ],
        salary: getRandomInt(30000, 150000),
        location: ["New York", "San Francisco", "London", "Tokyo", "Berlin"][
            getRandomInt(0, 4)
        ],
        joinDate: new Date(
            Date.now() - getRandomInt(0, 365 * 5) * 24 * 60 * 60 * 1000,
        ),
        isManager: Math.random() > 0.8,
        skills: getRandomArray(),
    };

    // Add 10-40 more random properties
    const additionalProps = getRandomInt(10, 40);
    for (let i = 0; i < additionalProps; i++) {
        user[`prop_${i}`] = generateRandomProperty();
    }

    return user;
}

// Generate a document with user reference and random properties
function generateDocument(userId: ObjectId): Record<string, any> {
    const doc: Record<string, any> = {
        user: userId,
        title: `Document ${getRandomString(8)}`,
        type: ["report", "invoice", "contract", "memo", "proposal"][
            getRandomInt(0, 4)
        ],
        status: ["draft", "published", "archived", "pending"][
            getRandomInt(0, 3)
        ],
        createdAt: new Date(
            Date.now() - getRandomInt(0, 365) * 24 * 60 * 60 * 1000,
        ),
        size: getRandomInt(1000, 50000),
        tags: getRandomArray(),
        metadata: getRandomObject(),
    };

    // Add 10-42 more random properties
    const additionalProps = getRandomInt(10, 42);
    for (let i = 0; i < additionalProps; i++) {
        doc[`doc_prop_${i}`] = generateRandomProperty();
    }

    return doc;
}

// Seed users collection with 100k documents
export async function seedUsers(
    db: Db,
    totalUsers: number = 100_000,
): Promise<ObjectId[]> {
    console.log("Starting to seed users...");
    const usersCollection = db.collection("users");

    // Clear existing data
    await usersCollection.deleteMany({});

    const userIds: ObjectId[] = [];

    for (let i = 0; i < totalUsers; i += batchSize) {
        const batch = [];
        const currentBatchSize = Math.min(batchSize, totalUsers - i);

        for (let j = 0; j < currentBatchSize; j++) {
            batch.push(generateUser());
        }

        const result = await usersCollection.insertMany(batch);
        userIds.push(...(Object.values(result.insertedIds) as ObjectId[]));

        if ((i + batchSize) % 10000 === 0 || i + batchSize >= totalUsers) {
            console.log(
                `Inserted ${Math.min(i + batchSize, totalUsers)} users`,
            );
        }
    }

    console.log(`Finished seeding ${totalUsers} users`);
    return userIds;
}

// Seed documents collection with specified documents per user using streaming approach
export async function seedDocuments(
    db: Db,
    userIds: ObjectId[],
    docsPerUser: number = 10,
): Promise<void> {
    console.log("Starting to seed documents...");
    const documentsCollection = db.collection("documents");

    // Clear existing data
    await documentsCollection.deleteMany({});

    const totalDocuments = userIds.length * docsPerUser;
    let totalInserted = 0;

    console.log(
        `Seeding ${totalDocuments} documents using streaming approach...`,
    );
    const startTime = Date.now();

    // Process users in chunks to avoid memory issues
    const userChunkSize = Math.floor(batchSize / docsPerUser) || 1; // At least 1 user per chunk

    for (
        let userStartIndex = 0;
        userStartIndex < userIds.length;
        userStartIndex += userChunkSize
    ) {
        const userEndIndex = Math.min(
            userStartIndex + userChunkSize,
            userIds.length,
        );
        const batch: Record<string, any>[] = [];

        // Generate documents for this chunk of users
        for (
            let userIndex = userStartIndex;
            userIndex < userEndIndex;
            userIndex++
        ) {
            const userId = userIds[userIndex];
            if (userId) {
                for (let docIndex = 0; docIndex < docsPerUser; docIndex++) {
                    batch.push(generateDocument(userId));
                }
            }
        }

        // Insert this batch
        if (batch.length > 0) {
            await documentsCollection.insertMany(batch);
            totalInserted += batch.length;
        }

        // Progress logging
        if (totalInserted % 50000 === 0 || userEndIndex >= userIds.length) {
            const progress = ((userEndIndex / userIds.length) * 100).toFixed(1);
            console.log(
                `Inserted ${totalInserted} documents (${progress}% complete)`,
            );
        }
    }

    const totalTime = Date.now() - startTime;
    console.log(
        `Finished seeding ${totalInserted} documents in ${totalTime / 1000} seconds`,
    );
}

// Main seeding function
export async function seedDatabase(
    db: Db,
    totalUsers: number = 100_000,
    docsPerUser: number = 10,
): Promise<void> {
    console.log("Starting database seeding...");
    const startTime = Date.now();

    // Seed users
    const userIds = await seedUsers(db, totalUsers);

    // Seed documents
    await seedDocuments(db, userIds, docsPerUser);

    const endTime = Date.now();
    console.log(
        `\nSeeding completed in ${(endTime - startTime) / 1000} seconds`,
    );
    console.log(`Total users: ${userIds.length}`);
    console.log(`Total documents: ${userIds.length * 1000}`);
}
