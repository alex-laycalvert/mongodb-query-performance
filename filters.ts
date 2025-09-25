import { Db } from "mongodb";

function getRandomInt(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Data distribution analysis results
interface DataDistribution {
    totalUsers: number;
    ageStats: { min: number; max: number; avg: number };
    salaryStats: { min: number; max: number; avg: number };
    statusCounts: Record<string, number>;
    departmentCounts: Record<string, number>;
    locationCounts: Record<string, number>;
    managerCount: number;
    joinDateStats: { min: Date; max: Date };
    emailDomainCounts: Record<string, number>;
}

let cachedDistribution: DataDistribution | null = null;

// Analyze user data to understand distribution
export async function analyzeUserDistribution(
    db: Db,
): Promise<DataDistribution> {
    if (cachedDistribution) {
        return cachedDistribution;
    }

    console.log("Analyzing user data distribution...");
    const usersCollection = db.collection("users");

    // Get basic stats without accumulating arrays
    const basicStatsPipeline = [
        {
            $group: {
                _id: null,
                totalUsers: { $sum: 1 },
                minAge: { $min: "$age" },
                maxAge: { $max: "$age" },
                avgAge: { $avg: "$age" },
                minSalary: { $min: "$salary" },
                maxSalary: { $max: "$salary" },
                avgSalary: { $avg: "$salary" },
                managerCount: { $sum: { $cond: ["$isManager", 1, 0] } },
                minJoinDate: { $min: "$joinDate" },
                maxJoinDate: { $max: "$joinDate" }
            },
        },
    ];

    // Get counts for categorical fields separately
    const statusCountsPipeline = [
        { $group: { _id: "$status", count: { $sum: 1 } } }
    ];

    const departmentCountsPipeline = [
        { $group: { _id: "$department", count: { $sum: 1 } } }
    ];

    const locationCountsPipeline = [
        { $group: { _id: "$location", count: { $sum: 1 } } }
    ];

    const emailDomainCountsPipeline = [
        {
            $project: {
                domain: { $arrayElemAt: [{ $split: ["$email", "@"] }, 1] }
            }
        },
        { $group: { _id: "$domain", count: { $sum: 1 } } }
    ];

    // Run all pipelines in parallel
    const [
        basicStatsResults,
        statusCountsResults,
        departmentCountsResults,
        locationCountsResults,
        emailDomainCountsResults
    ] = await Promise.all([
        usersCollection.aggregate(basicStatsPipeline).toArray(),
        usersCollection.aggregate(statusCountsPipeline).toArray(),
        usersCollection.aggregate(departmentCountsPipeline).toArray(),
        usersCollection.aggregate(locationCountsPipeline).toArray(),
        usersCollection.aggregate(emailDomainCountsPipeline).toArray()
    ]);

    const basicStats = basicStatsResults[0];
    if (!basicStats) {
        throw new Error("No user data found for analysis");
    }

    // Convert results to count objects
    const statusCounts: Record<string, number> = {};
    statusCountsResults.forEach((item: any) => {
        statusCounts[item._id] = item.count;
    });

    const departmentCounts: Record<string, number> = {};
    departmentCountsResults.forEach((item: any) => {
        departmentCounts[item._id] = item.count;
    });

    const locationCounts: Record<string, number> = {};
    locationCountsResults.forEach((item: any) => {
        locationCounts[item._id] = item.count;
    });

    const emailDomainCounts: Record<string, number> = {};
    emailDomainCountsResults.forEach((item: any) => {
        emailDomainCounts[item._id] = item.count;
    });

    cachedDistribution = {
        totalUsers: basicStats.totalUsers,
        ageStats: { min: basicStats.minAge, max: basicStats.maxAge, avg: basicStats.avgAge },
        salaryStats: {
            min: basicStats.minSalary,
            max: basicStats.maxSalary,
            avg: basicStats.avgSalary,
        },
        statusCounts,
        departmentCounts,
        locationCounts,
        managerCount: basicStats.managerCount,
        joinDateStats: { min: basicStats.minJoinDate, max: basicStats.maxJoinDate },
        emailDomainCounts,
    };

    console.log("Data distribution analysis complete");
    return cachedDistribution;
}

// Generate precise filter by sampling actual data and calculating exact boundaries
export async function generatePreciseUserFilter(
    db: Db,
    targetCount: number = 5000,
    previousAttempts: Array<{filter: Record<string, any>, count: number}> = []
): Promise<Record<string, any>> {
    const usersCollection = db.collection('users');
    const totalUsers = await usersCollection.countDocuments({});
    
    // If target count is >= total users, return match-all filter
    if (targetCount >= totalUsers) {
        return {};
    }
    
    const targetPercentage = targetCount / totalUsers;
    
    // Strategy 1: Age-based filter using percentiles
    if (Math.random() < 0.3) {
        const agePercentiles = await usersCollection.aggregate([
            { $sample: { size: 10000 } }, // Sample for performance
            { $sort: { age: 1 } },
            { $group: {
                _id: null,
                ages: { $push: "$age" }
            }}
        ]).toArray();
        
        const firstResult = agePercentiles[0];
        if (firstResult && firstResult.ages?.length > 0) {
            const ages = firstResult.ages;
            const startIndex = Math.floor(Math.random() * (ages.length * (1 - targetPercentage)));
            const endIndex = Math.min(startIndex + Math.ceil(ages.length * targetPercentage), ages.length - 1);
            
            return {
                age: {
                    $gte: ages[startIndex],
                    $lte: ages[endIndex]
                }
            };
        }
    }
    
    // Strategy 2: Salary-based filter using percentiles  
    if (Math.random() < 0.3) {
        const salaryPercentiles = await usersCollection.aggregate([
            { $sample: { size: 10000 } },
            { $sort: { salary: 1 } },
            { $group: {
                _id: null,
                salaries: { $push: "$salary" }
            }}
        ]).toArray();
        
        const firstSalaryResult = salaryPercentiles[0];
        if (firstSalaryResult && firstSalaryResult.salaries?.length > 0) {
            const salaries = firstSalaryResult.salaries;
            const startIndex = Math.floor(Math.random() * (salaries.length * (1 - targetPercentage)));
            const endIndex = Math.min(startIndex + Math.ceil(salaries.length * targetPercentage), salaries.length - 1);
            
            return {
                salary: {
                    $gte: salaries[startIndex],
                    $lte: salaries[endIndex]
                }
            };
        }
    }
    
    // Strategy 3: Categorical filters (status, department, location) with exact counts
    const categoricalStrategies = [
        'status', 'department', 'location'
    ];
    
    const field = categoricalStrategies[Math.floor(Math.random() * categoricalStrategies.length)];
    const counts = await usersCollection.aggregate([
        { $group: { _id: `$${field}`, count: { $sum: 1 } } },
        { $sort: { count: -1 } }
    ]).toArray();
    
    if (counts.length > 0) {
        // Find combination of values that gets closest to target
        let cumulative = 0;
        const selectedValues = [];
        
        for (const item of counts) {
            cumulative += item.count;
            selectedValues.push(item._id);
            
            // If we're close to target or exceeded it, stop
            if (cumulative >= targetCount) { // Target reached
                break;
            }
        }
        
        if (selectedValues.length === 1 && selectedValues[0] !== undefined) {
            if (field === 'status') return { status: selectedValues[0] };
            if (field === 'department') return { department: selectedValues[0] };
            if (field === 'location') return { location: selectedValues[0] };
        } else if (selectedValues.length > 1) {
            if (field === 'status') return { status: { $in: selectedValues } };
            if (field === 'department') return { department: { $in: selectedValues } };
            if (field === 'location') return { location: { $in: selectedValues } };
        }
    }
    
    // Strategy 4: Combined filters for fine-tuning
    if (targetCount < totalUsers * 0.2) { // For smaller target counts, use AND logic
        const strategies = [];
        
        // Add age constraint - be more conservative with range
        const ageRange = 80 - 18;
        const targetAgeRange = Math.max(5, Math.ceil(ageRange * Math.sqrt(targetPercentage * 0.8))); // Smaller range
        const startAge = 18 + Math.floor(Math.random() * (ageRange - targetAgeRange));
        strategies.push({ age: { $gte: startAge, $lte: startAge + targetAgeRange } });
        
        // Add department constraint for better targeting
        if (targetCount < totalUsers * 0.1) {
            const departments = ['engineering', 'marketing', 'sales'];
            const selectedDept = departments[Math.floor(Math.random() * departments.length)];
            strategies.push({ department: selectedDept });
        }
        
        // Add manager constraint for very small targets
        if (targetCount < totalUsers * 0.05) {
            strategies.push({ isManager: true });
        }
        
        return { $and: strategies };
    }
    
    // Fallback: Use simple age range
    const ageRangeSize = Math.ceil((80 - 18) * targetPercentage);
    const startAge = 18 + Math.floor(Math.random() * (62 - ageRangeSize));
    
    return {
        age: {
            $gte: startAge,
            $lte: startAge + ageRangeSize
        }
    };
}

// Generate and test filter using precise data analysis with retry logic
export async function generateOptimalUserFilter(
    db: Db,
    targetCount: number = 5000,
): Promise<{
    filter: Record<string, any>;
    count: number;
    targetCount: number;
}> {
    const usersCollection = db.collection("users");
    const marginOfError = Math.ceil(targetCount * 0.1);
    const minCount = targetCount;
    const maxCount = targetCount + marginOfError;
    
    let bestFilter: Record<string, any> = {};
    let bestCount = 0;
    let bestDistance = Infinity;
    const previousAttempts: Array<{filter: Record<string, any>, count: number}> = [];
    
    // Try up to 10 attempts to get within the 10% margin
    for (let attempt = 0; attempt < 10; attempt++) {
        const filter = await generatePreciseUserFilter(db, targetCount, previousAttempts);
        const count = await usersCollection.countDocuments(filter);
        
        previousAttempts.push({ filter, count });
        
        // Calculate distance from target range
        let distance = 0;
        if (count < minCount) {
            distance = minCount - count;
        } else if (count > maxCount) {
            distance = count - maxCount;
        }
        
        // If perfect match within range, return immediately
        if (distance === 0) {
            console.log(`Filter: ${JSON.stringify(filter)}`);
            console.log(`Target: ${targetCount}, Actual: ${count} users (✓ within 10% margin)`);
            return { filter, count, targetCount };
        }
        
        // Keep track of best attempt
        if (distance < bestDistance) {
            bestFilter = filter;
            bestCount = count;
            bestDistance = distance;
        }
    }
    
    // Return best attempt
    const withinRange = bestCount >= minCount && bestCount <= maxCount;
    console.log(`Filter: ${JSON.stringify(bestFilter)}`);
    console.log(`Target: ${targetCount}, Actual: ${bestCount} users ${withinRange ? '(✓ within 10% margin)' : '(⚠ outside 10% margin)'}`);

    return { filter: bestFilter, count: bestCount, targetCount };
}

// Generate multiple optimal test filters (fast and accurate)
export async function generateOptimalTestFilters(
    db: Db,
    targetCounts: number[] = [1000, 2500, 5000, 7500, 10000],
): Promise<
    Array<{ filter: Record<string, any>; count: number; targetCount: number }>
> {
    const filters = [];

    console.log(`\nGenerating ${targetCounts.length} precise test filters...`);

    for (let i = 0; i < targetCounts.length; i++) {
        const targetCount = targetCounts[i];
        if (targetCount !== undefined) {
            console.log(`\nGenerating filter for ${targetCount} users:`);
            
            const result = await generateOptimalUserFilter(db, targetCount);
            const marginOfError = Math.ceil(targetCount * 0.1);
            const withinRange = result.count >= targetCount && result.count <= targetCount + marginOfError;
            
            filters.push(result);
            console.log(
                `✓ Filter ${i + 1}: ${result.count} users ${withinRange ? '(✓ within 10% margin)' : '(⚠ outside 10% margin)'}`
            );
        }
    }

    return filters;
}

// Legacy function - deprecated
export function generateUserFilter(
    targetCount: number = 5000,
    distribution?: DataDistribution
): Record<string, any> {
    throw new Error('generateUserFilter is deprecated. Use generatePreciseUserFilter(db, targetCount) instead.');
}

// Legacy function for backward compatibility (slow)
export async function testUserFilter(
    db: Db,
    targetCount: number = 5000,
): Promise<{
    filter: Record<string, any>;
    count: number;
    targetCount: number;
}> {
    console.warn('Warning: testUserFilter is deprecated. Use generateOptimalUserFilter instead.');
    return generateOptimalUserFilter(db, targetCount);
}

// Legacy function for backward compatibility (slow) 
export async function generateTestFilters(
    db: Db,
    targetCounts: number[] = [1000, 2500, 5000, 7500, 10000],
): Promise<
    Array<{ filter: Record<string, any>; count: number; targetCount: number }>
> {
    console.warn('Warning: generateTestFilters is deprecated. Use generateOptimalTestFilters instead.');
    return generateOptimalTestFilters(db, targetCounts);
}