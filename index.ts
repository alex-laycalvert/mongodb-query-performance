import { seedDatabase } from "./seed";
import { generateOptimalTestFilters } from "./filters";
import assert from "node:assert";
import { closeMongoDB, connectToMongoDB } from "./db";
import { readFile, writeFile } from "node:fs/promises";

console.log("MongoDB Query Performance Testing");

const FIND_WITH_IN = "Find query with $in Documents Query Time";
const AGGREGATION_WITH_IN = "Aggregation with $in Query Time";
const AGGREGATION_WITH_LOOKUP = "Aggregation with $lookup Query Time";
const SKIP_PAGINATION_FIND_WITH_IN =
    "Find query with $in Documents Query Time (Skip paginated)";
const SKIP_PAGINATION_AGGREGATION_WITH_IN =
    "Aggregation with $in Query Time (Skip paginated)";
const SKIP_PAGINATION_AGGREGATION_WITH_LOOKUP =
    "Aggregation with $lookup Query Time (Skip paginated)";

type GeneratedFilters = {
    generatedAt: string;
    totalFilters: number;
    filters: {
        id: number;
        targetCount: number;
        actualCount: number;
        marginOfError: number;
        withinMargin: boolean;
        filter: Record<string, any>;
    }[];
};

// Main execution
async function main() {
    const db = await connectToMongoDB();

    if (process.argv.includes("--seed")) {
        // Seed the database
        await seedDatabase(db, 250_000, 100);
    }

    if (
        process.argv.includes("--seed") ||
        process.argv.includes("--seed-filters")
    ) {
        // Test filters with various target counts
        const filters = await generateOptimalTestFilters(
            db,
            [
                100, 1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 150_000,
                200_000,
            ],
        );

        // Write filters to JSON file
        console.log("\nWriting filters to filters.json...");
        const filtersData: GeneratedFilters = {
            generatedAt: new Date().toISOString(),
            totalFilters: filters.length,
            filters: filters.map((f, index) => ({
                id: index + 1,
                targetCount: f.targetCount,
                actualCount: f.count,
                marginOfError: Math.ceil(f.targetCount * 0.1),
                withinMargin:
                    f.count >= f.targetCount &&
                    f.count <= f.targetCount + Math.ceil(f.targetCount * 0.1),
                filter: f.filter,
            })),
        };

        await writeFile("./filters.json", JSON.stringify(filtersData, null, 2));
        console.log(`✓ Saved ${filters.length} filters to filters.json`);
    }

    const { filters }: GeneratedFilters = await readFile(
        "./filters.json",
        "utf-8",
    ).then((data) => JSON.parse(data));

    const usersCollection = db.collection("users");
    const documentsCollection = db.collection("documents");

    for (const { filter, actualCount, targetCount } of filters) {
        console.log(
            `\nQuerying documents for filter targeting ~${targetCount} users (actual: ${actualCount})...`,
        );

        {
            console.time(FIND_WITH_IN);
            const documents1 = await (async () => {
                const users1 = await usersCollection
                    .find(filter)
                    .project({ _id: 1 })
                    .toArray();
                const userIds1 = users1.map((user) => user._id);
                return await documentsCollection
                    .find({ userId: { $in: userIds1 } })
                    .sort({ _id: 1 })
                    .toArray();
            })();
            console.timeEnd(FIND_WITH_IN);

            console.time(AGGREGATION_WITH_IN);
            const documents2 = await (async () => {
                const users2 = await usersCollection
                    .find(filter)
                    .project({ _id: 1 })
                    .toArray();
                const userIds2 = users2.map((user) => user._id);
                return await documentsCollection
                    .aggregate([
                        { $match: { userId: { $in: userIds2 } } },
                        { $sort: { _id: 1 } },
                    ])
                    .toArray();
            })();
            console.timeEnd(AGGREGATION_WITH_IN);

            console.time(AGGREGATION_WITH_LOOKUP);
            const documents3 = await (async () => {
                return await documentsCollection
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
                                            $expr: {
                                                $eq: ["$userId", "$$userId"],
                                            },
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
            })();
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

        {
            const skip = Math.floor((actualCount * 100) / 2);
            const limit = Math.floor((actualCount * 100) / 4);

            console.time(SKIP_PAGINATION_FIND_WITH_IN);
            const [documents1, documents1Total] = await (async () => {
                const users1 = await usersCollection
                    .find(filter)
                    .project({ _id: 1 })
                    .toArray();
                const userIds1 = users1.map((user) => user._id);
                return await Promise.all([
                    documentsCollection
                        .find({ userId: { $in: userIds1 } })
                        .sort({ _id: 1 })
                        .skip(skip)
                        .limit(limit)
                        .toArray(),
                    documentsCollection.countDocuments({
                        userId: { $in: userIds1 },
                    }),
                ]);
            })();
            console.timeEnd(SKIP_PAGINATION_FIND_WITH_IN);

            console.time(SKIP_PAGINATION_AGGREGATION_WITH_IN);
            const [documents2, documents2Total] = await (async () => {
                const users2 = await usersCollection
                    .find(filter)
                    .project({ _id: 1 })
                    .toArray();
                const userIds2 = users2.map((user) => user._id);
                const [{ data: documents2, total: documents2Total }] =
                    await documentsCollection
                        .aggregate<any>([
                            { $match: { userId: { $in: userIds2 } } },
                            {
                                $facet: {
                                    data: [
                                        { $sort: { _id: 1 } },
                                        { $skip: skip },
                                        { $limit: limit },
                                    ],
                                    total: [{ $count: "total" }],
                                },
                            },
                        ])
                        .toArray();
                return [documents2, documents2Total?.[0]?.total ?? 0];
            })();
            console.timeEnd(SKIP_PAGINATION_AGGREGATION_WITH_IN);

            console.time(SKIP_PAGINATION_AGGREGATION_WITH_LOOKUP);
            const [documents3, documents3Total] = await (async () => {
                const [{ data: documents3, total: documents3Total }] =
                    await documentsCollection
                        .aggregate<any>([
                            {
                                $lookup: {
                                    from: "users",
                                    as: "user",
                                    let: { userId: "$userId" },
                                    pipeline: [
                                        { $match: filter },
                                        {
                                            $match: {
                                                $expr: {
                                                    $eq: [
                                                        "$userId",
                                                        "$$userId",
                                                    ],
                                                },
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
                            {
                                $facet: {
                                    data: [
                                        { $sort: { _id: 1 } },
                                        { $skip: skip },
                                        { $limit: limit },
                                    ],
                                    total: [{ $count: "total" }],
                                },
                            },
                        ])
                        .toArray();
                return [documents3, documents3Total?.[0]?.total ?? 0];
            })();
            console.timeEnd(SKIP_PAGINATION_AGGREGATION_WITH_LOOKUP);

            assert.equal(documents1.length, documents2.length);
            assert.equal(documents1.length, documents3.length);
            assert.equal(documents1Total, documents2Total);
            assert.equal(documents1Total, documents3Total);
            for (let i = 0; i < documents1.length; i++) {
                const d1 = documents1[i];
                const d2 = documents2[i];
                const d3 = documents3[i];
                assert.deepEqual(d1, d2);
                assert.deepEqual(d1, d3);
            }
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
