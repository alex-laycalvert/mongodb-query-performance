import { connectToMongoDB } from "./db";
import { ObjectId } from "mongodb";

interface QueryAnalysis {
    name: string;
    executionTimeMs: number;
    docsExamined: number;
    docsReturned: number;
    indexUsed: string;
    hasSortStage: boolean;
    efficiencyRatio: number;
}

function getIndexInfo(explain: any): string {
    function findIndexRecursive(stage: any): string | null {
        if (!stage) return null;
        
        // Check current stage
        if (stage.stage === "IXSCAN" && stage.indexName) {
            return stage.indexName;
        }
        
        // Check nested stages
        if (stage.inputStage) {
            return findIndexRecursive(stage.inputStage);
        }
        
        if (stage.inputStages && Array.isArray(stage.inputStages)) {
            for (let inputStage of stage.inputStages) {
                let result = findIndexRecursive(inputStage);
                if (result) return result;
            }
        }
        
        if (stage.executionStages) {
            return findIndexRecursive(stage.executionStages);
        }
        
        return null;
    }
    
    return findIndexRecursive(explain.executionStats.executionStages || explain.executionStats.winningPlan) || "COLLECTION_SCAN";
}

async function analyzeQuery(
    collection: any, 
    name: string, 
    query: any, 
    sort: any,
    hint?: any
): Promise<QueryAnalysis> {
    console.log(`\n--- ${name} ---`);
    
    let queryBuilder = collection.find(query).sort(sort).limit(1000);
    if (hint) {
        queryBuilder = queryBuilder.hint(hint);
        console.log(`Using hint: ${JSON.stringify(hint)}`);
    }
    
    const explain = await queryBuilder.explain("executionStats");
    
    const executionTimeMs = explain.executionStats.executionTimeMillis;
    const docsExamined = explain.executionStats.totalDocsExamined;
    const docsReturned = explain.executionStats.totalDocsReturned;
    const indexUsed = getIndexInfo(explain);
    const hasSortStage = JSON.stringify(explain).includes('"stage":"SORT"');
    const efficiencyRatio = docsExamined > 0 ? (docsReturned / docsExamined * 100) : 0;
    
    console.log(`Execution time: ${executionTimeMs}ms`);
    console.log(`Documents examined: ${docsExamined}`);
    console.log(`Documents returned: ${docsReturned}`);
    console.log(`Efficiency ratio: ${efficiencyRatio.toFixed(2)}%`);
    console.log(`Index used: ${indexUsed}`);
    
    if (hasSortStage) {
        console.log("⚠️  WARNING: In-memory sort detected - need better index!");
    }
    
    return {
        name,
        executionTimeMs,
        docsExamined,
        docsReturned,
        indexUsed,
        hasSortStage,
        efficiencyRatio
    };
}

async function checkIndexes(db: any): Promise<void> {
    console.log("\n" + "=".repeat(30));
    console.log("CURRENT INDEXES");
    console.log("=".repeat(30));
    
    const documentsIndexes = await db.collection("documents").indexes();
    const usersIndexes = await db.collection("users").indexes();
    
    console.log("\nDocuments collection indexes:");
    documentsIndexes.forEach((idx: any) => {
        console.log(`  ${idx.name}: ${JSON.stringify(idx.key)}`);
    });
    
    console.log("\nUsers collection indexes:");
    usersIndexes.forEach((idx: any) => {
        console.log(`  ${idx.name}: ${JSON.stringify(idx.key)}`);
    });
    
    // Check for critical indexes
    console.log("\n" + "=".repeat(30));
    console.log("INDEX RECOMMENDATIONS");
    console.log("=".repeat(30));
    
    const hasUserIdIndex = documentsIndexes.some((idx: any) => 
        JSON.stringify(idx.key) === '{"user":1,"_id":1}'
    );
    
    const hasUserCreatedAtIndex = documentsIndexes.some((idx: any) => 
        JSON.stringify(idx.key) === '{"user":1,"createdAt":-1,"_id":1}' ||
        JSON.stringify(idx.key) === '{"user":1,"createdAt":1,"_id":1}'
    );
    
    if (!hasUserIdIndex) {
        console.log("❌ Missing: db.collection('documents').createIndex({ user: 1, _id: 1 })");
    } else {
        console.log("✅ Has user + _id index");
    }
    
    if (!hasUserCreatedAtIndex) {
        console.log("❌ Missing: db.collection('documents').createIndex({ user: 1, createdAt: -1, _id: 1 })");
    } else {
        console.log("✅ Has user + createdAt + _id index");
    }
}

export async function debugQueryPerformance(): Promise<void> {
    console.log("=".repeat(50));
    console.log("MONGODB QUERY PERFORMANCE DEBUG");
    console.log("=".repeat(50));
    
    const db = await connectToMongoDB();
    const usersCollection = db.collection("users");
    const documentsCollection = db.collection("documents");
    
    // Get sample filter - use age range as it's common in your filters
    const sampleFilter = { age: { $gte: 25, $lte: 65 } };
    
    console.log("Getting sample user IDs with filter:", JSON.stringify(sampleFilter));
    
    const users = await usersCollection
        .find(sampleFilter, { projection: { _id: 1 } })
        .limit(1000)
        .toArray();
    
    const userIds = users.map(u => u._id);
    
    console.log(`Testing with ${userIds.length} user IDs`);
    
    if (userIds.length === 0) {
        console.log("❌ No users found with sample filter. Check your data or modify the filter.");
        return;
    }
    
    console.log("\n" + "=".repeat(30));
    console.log("TESTING CURSOR QUERIES WITHOUT HINTS");
    console.log("=".repeat(30));
    
    const analyses: QueryAnalysis[] = [];
    
    // 1. Test CURSOR_BY_ID (should be fast)
    const idAnalysis = await analyzeQuery(
        documentsCollection,
        "CURSOR BY ID",
        { user: { $in: userIds } },
        { _id: 1 }
    );
    analyses.push(idAnalysis);
    
    // 2. Test CURSOR_BY_DATE_DESC (probably slow without proper index)
    const dateAnalysis = await analyzeQuery(
        documentsCollection,
        "CURSOR BY DATE DESC",
        { user: { $in: userIds } },
        { createdAt: -1, _id: 1 }
    );
    analyses.push(dateAnalysis);
    
    console.log("\n" + "=".repeat(30));
    console.log("TESTING CURSOR QUERIES WITH HINTS");
    console.log("=".repeat(30));
    
    // Test with explicit index hints
    const idHintAnalysis = await analyzeQuery(
        documentsCollection,
        "CURSOR BY ID (with hint)",
        { user: { $in: userIds } },
        { _id: 1 },
        { user: 1, _id: 1 }
    );
    analyses.push(idHintAnalysis);
    
    const dateHintAnalysis = await analyzeQuery(
        documentsCollection,
        "CURSOR BY DATE DESC (with hint)",
        { user: { $in: userIds } },
        { createdAt: -1, _id: 1 },
        { user: 1, createdAt: -1, _id: 1 }
    );
    analyses.push(dateHintAnalysis);
    
    // 3. Test cursor with $or (next page scenario)
    const sampleDoc = await documentsCollection.findOne({ user: { $in: userIds } });
    
    if (sampleDoc && sampleDoc.createdAt) {
        const lastCreatedAt = sampleDoc.createdAt;
        const lastId = sampleDoc._id;
        
        const cursorAnalysis = await analyzeQuery(
            documentsCollection,
            "CURSOR WITH $OR (Next Page)",
            {
                user: { $in: userIds },
                $or: [
                    { createdAt: { $lt: lastCreatedAt } },
                    { 
                        createdAt: lastCreatedAt,
                        _id: { $lt: lastId }
                    }
                ]
            },
            { createdAt: -1, _id: 1 }
        );
        analyses.push(cursorAnalysis);
        
        // Test $or with hint
        const cursorHintAnalysis = await analyzeQuery(
            documentsCollection,
            "CURSOR WITH $OR (with hint)",
            {
                user: { $in: userIds },
                $or: [
                    { createdAt: { $lt: lastCreatedAt } },
                    { 
                        createdAt: lastCreatedAt,
                        _id: { $lt: lastId }
                    }
                ]
            },
            { createdAt: -1, _id: 1 },
            { user: 1, createdAt: -1, _id: 1 }
        );
        analyses.push(cursorHintAnalysis);
    } else {
        console.log("⚠️  No documents found with createdAt field for cursor test");
    }
    
    // Test with smaller user sets to see if $in array size is the issue
    console.log("\n" + "=".repeat(30));
    console.log("TESTING WITH SMALLER USER SETS");
    console.log("=".repeat(30));
    
    const smallUserIds = userIds.slice(0, 100); // Test with just 100 users
    console.log(`Testing with reduced user set: ${smallUserIds.length} users`);
    
    const smallSetAnalysis = await analyzeQuery(
        documentsCollection,
        "CURSOR BY DATE DESC (100 users)",
        { user: { $in: smallUserIds } },
        { createdAt: -1, _id: 1 },
        { user: 1, createdAt: -1, _id: 1 }
    );
    analyses.push(smallSetAnalysis);
    
    // Check current indexes
    await checkIndexes(db);
    
    // Summary
    console.log("\n" + "=".repeat(30));
    console.log("PERFORMANCE SUMMARY");
    console.log("=".repeat(30));
    
    analyses.forEach(analysis => {
        const status = analysis.executionTimeMs < 100 ? "✅ FAST" : 
                      analysis.executionTimeMs < 1000 ? "⚠️  SLOW" : "❌ VERY SLOW";
        console.log(`${analysis.name}: ${analysis.executionTimeMs}ms ${status}`);
        
        if (analysis.hasSortStage) {
            console.log(`  └─ 🐌 In-memory sort detected`);
        }
        
        if (analysis.efficiencyRatio < 10) {
            console.log(`  └─ 📊 Low efficiency: ${analysis.efficiencyRatio.toFixed(1)}%`);
        }
    });
    
    console.log("\n" + "=".repeat(50));
    console.log("DEBUG COMPLETE");
    console.log("=".repeat(50));
}
