import type { Collection } from "mongodb";
import { seedDatabase } from "./seed";
import { generateOptimalTestFilters } from "./filters";
import assert from "node:assert";
import { closeMongoDB, connectToMongoDB } from "./db";
import { readFile, writeFile } from "node:fs/promises";
import { parseArgs } from "node:util";

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

type NonPaginatedQueryMethod = (
    filter: Record<string, any>,
    usersCollection: Collection,
    documentsCollection: Collection,
) => Promise<any[]>;

const nonPaginatedMethods: { method: NonPaginatedQueryMethod; name: string }[] =
    [
        {
            method: findWithIn,
            name: "Find query with $in Documents Query Time",
        },
        {
            method: aggregationOnDocumentsWithIn,
            name: "Aggregation with $in Query Time",
        },
        {
            method: aggregationOnUsersWithLookup,
            name: "Aggregation on users with $lookup Query Time",
        },
        {
            method: aggregationOnUsersWithLookupStreaming,
            name: "Aggregation on users with $lookup (streaming) Query Time",
        },
        // {
        //     method: aggregationOnDocumentsWithLookup,
        //     name: "Aggregation on documents with $lookup Query Time",
        // },
    ];

type PaginatedQueryMethod = (
    filter: Record<string, any>,
    usersCollection: Collection,
    documentsCollection: Collection,
    skip: number,
    limit: number,
) => Promise<[any[], number]>;

const paginatedMethods: { method: PaginatedQueryMethod; name: string }[] = [
    {
        method: findWithInSkip,
        name: "Find query with $in Documents Query Time (Skip paginated)",
    },
    {
        method: aggregationOnDocumentsWithInSkip,
        name: "Aggregation with $in Query Time (Skip paginated)",
    },
    {
        method: aggregationOnUsersWithLookupSkip,
        name: "Aggregation on users with $lookup Query Time (Skip paginated)",
    },
    // {
    //     method: aggregationOnDocumentsWithLookupSkip,
    //     name: "Aggregation on documents with $lookup Query Time (Skip paginated)",
    // },
];

type QueryMethodResult<T> = {
    averageTimeMs: number;
    totalTimeMs: number;
    iterationTimesMs: number[];
    numIterations: number;
    lastResults: T;
};

// Main execution
async function main() {
    const db = await connectToMongoDB();

    const { values } = parseArgs({
        args: process.argv.slice(2),
        options: {
            seed: { type: "boolean" },
            "seed-filters": { type: "boolean" },
            "num-users": { type: "string", short: "u", default: "10_000" },
            "num-docs-per-user": { type: "string", short: "d", default: "10" },
            output: { type: "string", short: "o", default: "./results.json" },
            iterations: { type: "string", short: "i", default: "1" },
        },
    });

    if (values.seed) {
        // Seed the database
        const numUsers = +values["num-users"];
        const numDocsPerUser = +values["num-docs-per-user"];
        await seedDatabase(db, numUsers, numDocsPerUser);
    }

    const usersCollection = db.collection("users");
    const documentsCollection = db.collection("documents");
    const maxUsers = await usersCollection.estimatedDocumentCount();

    if (values.seed || values["seed-filters"]) {
        // Test filters with various target counts
        const filters = await generateOptimalTestFilters(
            db,
            [100, 1_000, 5_000, 10_000, 25_000, 50_000, 75_000, 100_000].filter(
                (n) => n <= maxUsers,
            ),
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

    console.log("\nRunning tests with the following parameters:");
    console.log(`  - Iterations per method: ${values.iterations}`);

    const filterResults: {
        filter: Record<string, any>;
        targetCount: number;
        actualCount: number;
        results: (Omit<QueryMethodResult<any>, "lastResults"> & {})[];
    }[] = [];
    for (const { filter, actualCount, targetCount } of filters) {
        console.log(
            `\nQuerying documents for filter targeting ~${targetCount} users (actual: ${actualCount})...`,
        );

        const nonPaginatedResults: Record<
            string,
            QueryMethodResult<any[]>
        > = {};
        for (const method of nonPaginatedMethods) {
            console.log(`  - Testing method: ${method.name}`);
            for (let i = 0; i < +values.iterations; i++) {
                const start = Date.now();
                const results = await method.method(
                    filter,
                    usersCollection,
                    documentsCollection,
                );
                const duration = Date.now() - start;
                if (!nonPaginatedResults[method.name]) {
                    nonPaginatedResults[method.name] = {
                        lastResults: results,
                        numIterations: 0,
                        averageTimeMs: 0,
                        iterationTimesMs: [],
                        totalTimeMs: 0,
                    };
                } else {
                    assert.equal(
                        results.length,
                        nonPaginatedResults[method.name]!.lastResults.length,
                    );
                    for (let j = 0; j < results.length; j++) {
                        const r = results[j];
                        const lr =
                            nonPaginatedResults[method.name]!.lastResults[j];
                        assert.deepEqual(r, lr);
                    }
                }
                nonPaginatedResults[method.name]!.lastResults = results;
                nonPaginatedResults[method.name]!.totalTimeMs += duration;
                nonPaginatedResults[method.name]!.iterationTimesMs.push(
                    duration,
                );
                nonPaginatedResults[method.name]!.numIterations++;
            }
            const totalTime = nonPaginatedResults[method.name]!.totalTimeMs;
            const iterations = nonPaginatedResults[method.name]!.numIterations;
            nonPaginatedResults[method.name]!.averageTimeMs =
                totalTime / iterations;
        }

        // Ensure all methods have the same results
        const nonPaginatedMethodResults = Object.values(nonPaginatedResults);
        const nonPaginatedFirstResults =
            nonPaginatedMethodResults[0]!.lastResults;
        for (let i = 1; i < nonPaginatedMethodResults.length; i++) {
            assert.equal(
                nonPaginatedMethodResults[i]!.lastResults.length,
                nonPaginatedFirstResults.length,
            );
            for (let j = 0; j < nonPaginatedFirstResults.length; j++) {
                assert.deepEqual(
                    nonPaginatedMethodResults[i]!.lastResults[j],
                    nonPaginatedFirstResults[j],
                );
            }
        }

        const skip = Math.floor((actualCount * 100) / 2);
        const limit = Math.floor((actualCount * 100) / 4);
        const paginatedResults: Record<
            string,
            QueryMethodResult<[any[], number]>
        > = {};
        for (const method of paginatedMethods) {
            console.log(`  - Testing method: ${method.name}`);
            for (let i = 0; i < +values.iterations; i++) {
                const start = Date.now();
                const results = await method.method(
                    filter,
                    usersCollection,
                    documentsCollection,
                    skip,
                    limit,
                );
                const duration = Date.now() - start;
                if (!paginatedResults[method.name]) {
                    paginatedResults[method.name] = {
                        lastResults: results,
                        numIterations: 0,
                        averageTimeMs: 0,
                        iterationTimesMs: [],
                        totalTimeMs: 0,
                    };
                } else {
                    assert.equal(
                        results.length,
                        paginatedResults[method.name]!.lastResults.length,
                    );
                    for (let j = 0; j < results.length; j++) {
                        const r = results[j];
                        const lr =
                            paginatedResults[method.name]!.lastResults[j];
                        assert.deepEqual(r, lr);
                    }
                }
                paginatedResults[method.name]!.lastResults = results;
                paginatedResults[method.name]!.totalTimeMs += duration;
                paginatedResults[method.name]!.iterationTimesMs.push(duration);
                paginatedResults[method.name]!.numIterations++;
            }
            const totalTime = paginatedResults[method.name]!.totalTimeMs;
            const iterations = paginatedResults[method.name]!.numIterations;
            paginatedResults[method.name]!.averageTimeMs =
                totalTime / iterations;
        }

        // Ensure all methods have the same results
        const paginatedMethodResults = Object.values(paginatedResults);
        const paginatedFirstResults = paginatedMethodResults[0]!.lastResults;
        for (let i = 1; i < paginatedMethodResults.length; i++) {
            assert.equal(
                paginatedMethodResults[i]!.lastResults[0].length,
                paginatedFirstResults[0].length,
            );
            assert.equal(
                paginatedMethodResults[i]!.lastResults[1],
                paginatedFirstResults[1],
            );
            for (let j = 0; j < paginatedFirstResults[0].length; j++) {
                assert.deepEqual(
                    paginatedMethodResults[i]!.lastResults[0][j],
                    paginatedFirstResults[0][j],
                );
            }
        }

        const formattedResults = Object.entries({
            ...nonPaginatedResults,
            ...paginatedResults,
        }).reduce<
            (Omit<QueryMethodResult<any>, "lastResults"> & {
                methodName: string;
            })[]
        >((acc, [methodName, result]) => {
            acc.push({
                methodName,
                averageTimeMs: result.averageTimeMs,
                totalTimeMs: result.totalTimeMs,
                numIterations: result.numIterations,
                iterationTimesMs: result.iterationTimesMs,
            });
            return acc;
        }, []);

        filterResults.push({
            filter,
            targetCount,
            actualCount,
            results: formattedResults,
        });
    }

    // Write results to JSON file
    console.log(`\nWriting results to ${values.output}...`);
    await writeFile(values.output, JSON.stringify(filterResults, null, 2));

    await closeMongoDB();
    console.log("\nCompleted successfully!");
}

async function findWithIn(
    filter: Record<string, any>,
    usersCollection: Collection,
    documentsCollection: Collection,
) {
    const users1 = await usersCollection
        .find(filter)
        .project({ _id: 1 })
        .toArray();
    const userIds1 = users1.map((user) => user._id);
    return await documentsCollection
        .find({ user: { $in: userIds1 } })
        .sort({ _id: 1 })
        .toArray();
}

async function aggregationOnDocumentsWithIn(
    filter: Record<string, any>,
    usersCollection: Collection,
    documentsCollection: Collection,
) {
    const users2 = await usersCollection
        .find(filter)
        .project({ _id: 1 })
        .toArray();
    const userIds2 = users2.map((user) => user._id);
    return await documentsCollection
        .aggregate(
            [{ $match: { user: { $in: userIds2 } } }, { $sort: { _id: 1 } }],
            { allowDiskUse: true },
        )
        .toArray();
}

async function aggregationOnUsersWithLookup(
    filter: Record<string, any>,
    usersCollection: Collection,
    _documentsCollection: Collection,
) {
    return await usersCollection
        .aggregate(
            [
                { $match: filter },
                {
                    $lookup: {
                        from: "documents",
                        as: "documents",
                        localField: "_id",
                        foreignField: "user",
                    },
                },
                { $unwind: "$documents" },
                { $replaceRoot: { newRoot: "$documents" } },
                { $sort: { _id: 1 } },
            ],
            { allowDiskUse: true },
        )
        .toArray();
}

async function aggregationOnUsersWithLookupStreaming(
    filter: Record<string, any>,
    usersCollection: Collection,
    _documentsCollection: Collection,
) {
    const data: any[] = [];
    const cursor = usersCollection.aggregate(
        [
            { $match: filter },
            {
                $lookup: {
                    from: "documents",
                    as: "documents",
                    localField: "_id",
                    foreignField: "user",
                },
            },
            { $unwind: "$documents" },
            { $replaceRoot: { newRoot: "$documents" } },
            { $sort: { _id: 1 } },
        ],
        { batchSize: 1000, allowDiskUse: true },
    );

    for await (const doc of cursor) {
        data.push(doc);
    }

    return data;
}

async function aggregationOnDocumentsWithLookup(
    filter: Record<string, any>,
    _usersCollection: Collection,
    documentsCollection: Collection,
) {
    return await documentsCollection
        .aggregate(
            [
                { $sort: { _id: 1 } },
                {
                    $lookup: {
                        from: "users",
                        as: "users",
                        let: { userId: "$user" },
                        pipeline: [
                            {
                                $match: {
                                    $expr: {
                                        $eq: ["$_id", "$$userId"],
                                    },
                                },
                            },
                            { $match: filter },
                            { $limit: 1 },
                        ],
                    },
                },
                {
                    $match: {
                        "users.0": { $exists: true },
                    },
                },
                { $project: { users: 0 } },
            ],
            { allowDiskUse: true },
        )
        .toArray();
}

async function findWithInSkip(
    filter: Record<string, any>,
    usersCollection: Collection,
    documentsCollection: Collection,
    skip: number,
    limit: number,
): Promise<[any[], number]> {
    const users1 = await usersCollection
        .find(filter)
        .project({ _id: 1 })
        .toArray();
    const userIds1 = users1.map((user) => user._id);
    return await Promise.all([
        documentsCollection
            .find({ user: { $in: userIds1 } })
            .sort({ _id: 1 })
            .skip(skip)
            .limit(limit)
            .toArray(),
        documentsCollection.countDocuments({
            user: { $in: userIds1 },
        }),
    ]);
}

async function aggregationOnDocumentsWithInSkip(
    filter: Record<string, any>,
    usersCollection: Collection,
    documentsCollection: Collection,
    skip: number,
    limit: number,
): Promise<[any[], number]> {
    const users2 = await usersCollection
        .find(filter)
        .project({ _id: 1 })
        .toArray();
    const userIds2 = users2.map((user) => user._id);
    const [{ data: documents2, total: documents2Total }] =
        await documentsCollection
            .aggregate<any>(
                [
                    { $match: { user: { $in: userIds2 } } },
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
                ],
                { allowDiskUse: true },
            )
            .toArray();
    return [documents2, documents2Total?.[0]?.total ?? 0];
}
async function aggregationOnUsersWithLookupSkip(
    filter: Record<string, any>,
    usersCollection: Collection,
    _documentsCollection: Collection,
    skip: number,
    limit: number,
): Promise<[any[], number]> {
    const [{ data: documents3, total: documents3Total }] = await usersCollection
        .aggregate<any>(
            [
                { $match: filter },
                {
                    $lookup: {
                        from: "documents",
                        as: "documents",
                        localField: "_id",
                        foreignField: "user",
                    },
                },
                { $unwind: "$documents" },
                { $replaceRoot: { newRoot: "$documents" } },
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
            ],
            { allowDiskUse: true },
        )
        .toArray();
    return [documents3, documents3Total?.[0]?.total ?? 0];
}

async function aggregationOnDocumentsWithLookupSkip(
    filter: Record<string, any>,
    _usersCollection: Collection,
    documentsCollection: Collection,
    skip: number,
    limit: number,
): Promise<[any[], number]> {
    const [{ data, total }] = await documentsCollection
        .aggregate<any>(
            [
                { $sort: { _id: 1 } },
                {
                    $lookup: {
                        from: "users",
                        as: "users",
                        let: { userId: "$user" },
                        pipeline: [
                            {
                                $match: {
                                    $expr: {
                                        $eq: ["$_id", "$$userId"],
                                    },
                                },
                            },
                            { $match: filter },
                            { $limit: 1 },
                        ],
                    },
                },
                {
                    $match: {
                        "users.0": { $exists: true },
                    },
                },
                { $project: { users: 0 } },
                {
                    $facet: {
                        data: [{ $skip: skip }, { $limit: limit }],
                        total: [{ $count: "total" }],
                    },
                },
            ],
            { allowDiskUse: true },
        )
        .toArray();

    return [data, total?.[0]?.total ?? 0];
}

// Run if executed directly
if (import.meta.main) {
    console.log("MongoDB Query Performance Testing");

    main().catch((err: unknown) => {
        console.error("Error in main execution:", err);
        process.exit(1);
    });
}
