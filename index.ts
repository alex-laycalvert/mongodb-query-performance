import { seedDatabase } from "./seed";
import { generateTestFilters } from "./filters";
import { closeMongoDB, connectToMongoDB } from "./db";
import assert from "node:assert";

console.log("MongoDB Query Performance Testing");

const FIND_WITH_IN = "User Filter + documents.find + $in Documents Query Time";
const AGGREGATION_WITH_IN = "Aggregation with $in Query Time";
const AGGREGATION_WITH_LOOKUP = "Aggregation with $lookup Query Time";

// Main execution
async function main() {
    const db = await connectToMongoDB();

    if (process.argv.includes("--seed")) {
        // Seed the database
        await seedDatabase(db, 250_000, 100);
    }

    // Connect and test filters
    console.log("\nConnecting to test filters...");

    console.log("\nTesting user filters with different target counts...");

    // Test filters with various target counts
    const filters = await generateTestFilters(
        db,
        [
            10, 100, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000,
            150_000, 200_000,
        ],
    );

    const usersCollection = db.collection("users");
    const documentsCollection = db.collection("documents");

    for (const { filter, count, targetCount } of filters) {
        console.log(
            `\nQuerying documents for filter targeting ~${targetCount} users (actual: ${count})...`,
        );

        // 1. Filter users collection first, then use $in to get documents
        console.time(FIND_WITH_IN);
        const users1 = await usersCollection
            .find(filter)
            .project({ _id: 1 })
            .toArray();
        const userIds1 = users1.map((user) => user._id);
        const documents1 = await documentsCollection
            .find({ userId: { $in: userIds1 } })
            .sort({ _id: 1 })
            .toArray();
        console.timeEnd(FIND_WITH_IN);

        // 2. Use aggregation with $lookup to join users and documents
        console.time(AGGREGATION_WITH_IN);
        const users2 = await usersCollection
            .find(filter)
            .project({ _id: 1 })
            .toArray();
        const userIds2 = users2.map((user) => user._id);
        const documents2 = await documentsCollection
            .aggregate([
                { $match: { userId: { $in: userIds2 } } },
                { $sort: { _id: 1 } },
            ])
            .toArray();
        console.timeEnd(AGGREGATION_WITH_IN);

        // 2. Use aggregation with $lookup to join users and documents
        console.time(AGGREGATION_WITH_LOOKUP);
        const documents3 = await documentsCollection
            .aggregate([
                {
                    $lookup: {
                        from: "users",
                        as: "user",
                        let: { userId: "$userId" },
                        pipeline: [
                            { $match: filter },
                            {
                                $match: {
                                    $expr: { $eq: ["$userId", "$$userId"] },
                                },
                            },
                            { $limit: 1 },
                        ],
                    },
                },
                {
                    $match: {
                        "users.0": { $exists: true },
                    },
                },
                { $project: { user: 0 } },
                { $sort: { _id: 1 } },
            ])
            .toArray();
        console.timeEnd(AGGREGATION_WITH_LOOKUP);

        assert.equal(documents1.length, documents2.length);
        assert.equal(documents1.length, documents3.length);
        for (let i = 0; i < documents1.length; i++) {
            const d1 = documents1[i];
            const d2 = documents2[i];
            const d3 = documents3[i];
            assert.deepEqual(d1, d2);
            assert.deepEqual(d1, d3);
        }
    }

    await closeMongoDB();
    console.log("\nCompleted successfully!");
}

// Run if executed directly
if (import.meta.main) {
    main().catch((err: unknown) => {
        console.error("Error in main execution:", err);
        process.exit(1);
    });
}

