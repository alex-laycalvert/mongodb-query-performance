import { seedDatabase } from "./seed";
import { generateOptimalTestFilters } from "./filters";
import { debugQueryPerformance } from "./debug";
import assert from "node:assert";
import { closeMongoDB, connectToMongoDB } from "./db";
import { readFile, writeFile } from "node:fs/promises";
import { ObjectId } from "mongodb";

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

const BATCH_SIZE = 100;
const MAX_USER_IDS_FOR_CURSOR = 500; // Limit user IDs for optimal cursor performance

async function getBatchedUserIds(collection: any, filter: any) {
    const userIds: any[] = [];
    const cursor = collection
        .find(filter)
        .project({ _id: 1 })
        .batchSize(BATCH_SIZE);

    for await (const user of cursor) {
        userIds.push(user._id);
    }

    return userIds;
}

async function getOptimalUserIds(collection: any, filter: any, maxUsers: number = MAX_USER_IDS_FOR_CURSOR) {
    const userIds = await getBatchedUserIds(collection, filter);
    
    if (userIds.length <= maxUsers) {
        return userIds;
    }
    
    console.log(`⚠️  Large user set (${userIds.length}), limiting to ${maxUsers} for optimal cursor performance`);
    return userIds.slice(0, maxUsers);
}

async function getBatchedDocuments(
    collection: any,
    userIds: any[],
    sort: any = { _id: 1 },
) {
    const documents: any[] = [];

    // Process in batches to avoid large $in arrays
    for (let i = 0; i < userIds.length; i += BATCH_SIZE) {
        const batch = userIds.slice(i, i + BATCH_SIZE);
        const batchDocs = await collection
            .find({ user: { $in: batch } })
            .sort(sort)
            .toArray();
        documents.push(...batchDocs);
    }

    // Sort final result if needed
    if (sort._id) {
        documents.sort((a, b) =>
            a._id.toString().localeCompare(b._id.toString()),
        );
    }

    return documents;
}

async function getSingleColumnCursorPagination(
    documentsCollection: any,
    userIds: any[],
    sortField: string = '_id',
    sortDirection: 1 | -1 = 1,
    limit: number = 1000,
    lastValue?: any,
    lastId?: string
) {
    let query: any = { user: { $in: userIds } };
    
    if (lastValue !== undefined && lastId) {
        const operator = sortDirection === 1 ? '$gt' : '$lt';
        const equalOperator = sortDirection === 1 ? '$gt' : '$lt';
        
        if (sortField === '_id') {
            // Simple case: sorting by _id only
            query._id = { [operator]: new ObjectId(lastId) };
        } else {
            // Handle ties with _id as tie-breaker
            query = {
                user: { $in: userIds },
                $or: [
                    { [sortField]: { [operator]: lastValue } },
                    { 
                        [sortField]: lastValue,
                        _id: { [equalOperator]: new ObjectId(lastId) }
                    }
                ]
            };
        }
    }
    
    const sortObj = sortField === '_id' 
        ? { _id: sortDirection }
        : { [sortField]: sortDirection, _id: sortDirection };
    
    // Determine the optimal index hint
    const indexHint = sortField === '_id' 
        ? { user: 1, _id: 1 }
        : { user: 1, [sortField]: sortDirection, _id: 1 };
    
    const documents = await documentsCollection
        .find(query)
        .sort(sortObj)
        .hint(indexHint)
        .limit(limit)
        .toArray();

    console.log(documents.length, 'documents fetched with cursor pagination');
    
    return {
        documents,
        nextCursor: documents.length > 0 ? {
            value: documents[documents.length - 1][sortField],
            _id: documents[documents.length - 1]._id.toString()
        } : null,
        hasMore: documents.length === limit
    };
}

// Main execution
async function main() {
    const db = await connectToMongoDB();

    if (process.argv.includes("--debug")) {
        // Run query performance debugging
        await debugQueryPerformance();
        await closeMongoDB();
        return;
    }

    if (process.argv.includes("--seed")) {
        // Seed the database
        await seedDatabase(db, 50_000, 100);
    }

    if (
        process.argv.includes("--seed") ||
        process.argv.includes("--seed-filters")
    ) {
        // Test filters with various target counts
        const filters = await generateOptimalTestFilters(
            db,
            [100, 1_000, 5_000, 10_000, 25_000, 50_000],
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
            try {
                const documents1 = await (async () => {
                    const users1 = await usersCollection
                        .find(filter, { projection: { _id: 1 } })
                        .toArray();
                    const userIds1 = users1.map((user) => user._id);
                    return await documentsCollection
                        .find({ user: { $in: userIds1 } })
                        .sort({ _id: 1 })
                        .toArray();
                })();
                console.log(documents1.length, 'documents fetched with $in');
            } catch (err) {
                console.error("Error during find with $in");
            } finally {
                console.timeEnd(FIND_WITH_IN);
            }
/*
            console.time(AGGREGATION_WITH_IN);
            try {
                const documents2 = await (async () => {
                    const users2 = await usersCollection
                        .find(filter)
                        .project({ _id: 1 })
                        .toArray();
                    const userIds2 = users2.map((user) => user._id);
                    return await documentsCollection
                        .aggregate(
                            [
                                { $match: { user: { $in: userIds2 } } },
                                { $sort: { _id: 1 } },
                            ],
                            { allowDiskUse: true, batchSize: 1000 },
                        )
                        .toArray();
                })();
            } catch (err) {
                console.error("Error during aggregation with $in");
            } finally {
                console.timeEnd(AGGREGATION_WITH_IN);
            }

            console.time(AGGREGATION_WITH_LOOKUP);
            try {
                const documents3 = await (async () => {
                    return await usersCollection
                        .aggregate(
                            [
                                { $match: filter },
                                {
                                    $lookup: {
                                        from: "documents",
                                        localField: "_id",
                                        foreignField: "user",
                                        as: "documents",
                                    },
                                },
                                { $unwind: "$documents" },
                                { $replaceRoot: { newRoot: "$documents" } },
                                { $sort: { _id: 1 } },
                            ],
                            { allowDiskUse: true, batchSize: 1000 },
                        )
                        .toArray();
                })();
            } catch (err) {
                console.error("Error during aggregation with $lookup");
            } finally {
                console.timeEnd(AGGREGATION_WITH_LOOKUP);
            }*/

            /*assert.equal(documents1.length, documents2.length);
            assert.equal(documents1.length, documents3.length);
            for (let i = 0; i < documents1.length; i++) {
                const d1 = documents1[i];
                const d2 = documents2[i];
                const d3 = documents3[i];
                assert.deepEqual(d1, d2);
                assert.deepEqual(d1, d3);
            }*/
        }

        {
            /*console.time("OPTIMIZED_BATCHED_FIND");
            const documents1 = await (async () => {
                const userIds = await getBatchedUserIds(
                    usersCollection,
                    filter,
                );
                return await getBatchedDocuments(documentsCollection, userIds);
            })();
            console.timeEnd("OPTIMIZED_BATCHED_FIND");

            // For very large result sets, use streaming aggregation
            console.time("STREAMING_AGGREGATION");
            const documents2: any[] = [];
            const cursor = usersCollection.aggregate(
                [
                    { $match: filter },
                    {
                        $lookup: {
                            from: "documents",
                            localField: "_id",
                            foreignField: "user",
                            as: "documents",
                        },
                    },
                    { $unwind: "$documents" },
                    { $replaceRoot: { newRoot: "$documents" } },
                    { $sort: { _id: 1 } },
                ],
                { allowDiskUse: true, batchSize: 1000 },
            );

            for await (const doc of cursor) {
                documents2.push(doc);
            }
            console.timeEnd("STREAMING_AGGREGATION");
            */
        }

        {
            // Sort by _id (simplest case)
            console.time("CURSOR_BY_ID");
            const users1 = await usersCollection
                .find(filter, { projection: { _id: 1 } })
                .toArray();
            const userIds1 = users1.map((user) => user._id);
            const idResult = await getSingleColumnCursorPagination(
                documentsCollection,
                userIds1,
                '_id',
                1,
                1000
            );
            console.timeEnd("CURSOR_BY_ID");
            
            // Sort by createdAt descending
            console.time("CURSOR_BY_DATE_DESC");
            const users2 = await usersCollection
                .find(filter, { projection: { _id: 1 } })
                .toArray();
            const userIds2 = users2.map((user) => user._id);
            const dateResult = await getSingleColumnCursorPagination(
                documentsCollection,
                userIds2,
                'createdAt',
                -1,
                1000
            );
            console.timeEnd("CURSOR_BY_DATE_DESC");
            
            // Get next page using cursor
            if (dateResult.nextCursor) {
                console.time("CURSOR_BY_DATE_DESC_NEXT_PAGE");
                const users3 = await usersCollection
                    .find(filter, { projection: { _id: 1 } })
                    .toArray();
                const userIds3 = users3.map((user) => user._id);
                const nextPage = await getSingleColumnCursorPagination(
                    documentsCollection,
                    userIds3,
                    'createdAt',
                    -1,
                    1000,
                    dateResult.nextCursor.value,
                    dateResult.nextCursor._id
                );
                console.timeEnd("CURSOR_BY_DATE_DESC_NEXT_PAGE");
            }
        }

        {
            const skip = Math.floor((actualCount * 100) / 2);
            const limit = Math.floor((actualCount * 100) / 4);

            console.time(SKIP_PAGINATION_FIND_WITH_IN);
            try {
                const [documents1, documents1Total] = await (async () => {
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
                })();
            } catch (err) {
                console.error(
                    "Error during find with $in and pagination:"
                );
            } finally {
                console.timeEnd(SKIP_PAGINATION_FIND_WITH_IN);
            }
/*
            console.time(SKIP_PAGINATION_AGGREGATION_WITH_IN);
            try {
                const [documents2, documents2Total] = await (async () => {
                    console.log(
                        "Starting aggregation with $in and pagination...",
                    );
                    const users2 = await usersCollection
                        .find(filter)
                        .project({ _id: 1 })
                        .toArray();
                    const userIds2 = users2.map((user) => user._id);
                    console.log(
                        `Found ${userIds2.length} user IDs for aggregation.`,
                    );
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
                                { allowDiskUse: true, batchSize: 1000 },
                            )
                            .toArray();
                    return [documents2, documents2Total?.[0]?.total ?? 0];
                })();
            } catch (err) {
                console.error(
                    "Error during aggregation with $in and pagination:"
                );
            } finally {
                console.timeEnd(SKIP_PAGINATION_AGGREGATION_WITH_IN);
            }

            console.time(SKIP_PAGINATION_AGGREGATION_WITH_LOOKUP);
            try {
                const [documents3, documents3Total] = await (async () => {
                    const [{ data: documents3, total: documents3Total }] =
                        await usersCollection
                            .aggregate<any>(
                                [
                                    { $match: filter },
                                    {
                                        $lookup: {
                                            from: "documents",
                                            localField: "_id",
                                            foreignField: "user",
                                            as: "documents",
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
                                { allowDiskUse: true, batchSize: 1000 },
                            )
                            .toArray();
                    return [documents3, documents3Total?.[0]?.total ?? 0];
                })();
            } catch (err) {
                console.error(
                    "Error during aggregation with $lookup and pagination"
                );
            } finally {
                console.timeEnd(SKIP_PAGINATION_AGGREGATION_WITH_LOOKUP);
            }*/

            /*assert.equal(documents1.length, documents2.length);
            assert.equal(documents1.length, documents3.length);
            assert.equal(documents1Total, documents2Total);
            assert.equal(documents1Total, documents3Total);
            for (let i = 0; i < documents1.length; i++) {
                const d1 = documents1[i];
                const d2 = documents2[i];
                const d3 = documents3[i];
                assert.deepEqual(d1, d2);
                assert.deepEqual(d1, d3);
            }*/
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
