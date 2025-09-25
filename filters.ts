import { Db } from 'mongodb';

function getRandomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Generate filter that returns approximately the specified number of users (min: targetCount, max: targetCount + 100)
export function generateUserFilter(targetCount: number = 5000): Record<string, any> {
  // Calculate percentage ranges based on target count for better accuracy
  const totalUsers = 100000;
  const targetPercentage = targetCount / totalUsers;
  
  const filterOptions = [
    // Age range filters - adjust ranges based on target
    () => {
      const ageRange = Math.ceil(targetPercentage * 62); // 62 is max age range (80-18)
      const startAge = getRandomInt(18, 80 - ageRange);
      return { age: { $gte: startAge, $lte: startAge + ageRange } };
    },
    
    // Salary range filters - adjust ranges based on target
    () => {
      const salaryRange = Math.ceil(targetPercentage * 120000); // 120k is max salary range
      const minSalary = getRandomInt(30000, 150000 - salaryRange);
      return { salary: { $gte: minSalary, $lte: minSalary + salaryRange } };
    },
    
    // Status filters - roughly 25% each status
    () => {
      if (targetPercentage <= 0.25) {
        return { status: ['active', 'inactive', 'pending', 'banned'][getRandomInt(0, 3)] };
      } else if (targetPercentage <= 0.5) {
        const statuses = ['active', 'inactive', 'pending', 'banned'];
        return { status: { $in: [statuses[getRandomInt(0, 3)], statuses[getRandomInt(0, 3)]] } };
      } else {
        return { status: { $in: ['active', 'pending', 'inactive'] } };
      }
    },
    
    // Department filters - roughly 20% each department
    () => {
      if (targetPercentage <= 0.2) {
        return { department: ['engineering', 'marketing', 'sales', 'support', 'hr'][getRandomInt(0, 4)] };
      } else if (targetPercentage <= 0.4) {
        return { department: { $in: ['engineering', 'marketing'] } };
      } else if (targetPercentage <= 0.6) {
        return { department: { $in: ['engineering', 'marketing', 'sales'] } };
      } else {
        return { department: { $in: ['engineering', 'marketing', 'sales', 'support'] } };
      }
    },
    
    // Location filters - roughly 20% each location
    () => {
      if (targetPercentage <= 0.2) {
        return { location: ['New York', 'San Francisco', 'London', 'Tokyo', 'Berlin'][getRandomInt(0, 4)] };
      } else if (targetPercentage <= 0.4) {
        return { location: { $in: ['New York', 'San Francisco'] } };
      } else if (targetPercentage <= 0.6) {
        return { location: { $in: ['New York', 'San Francisco', 'London'] } };
      } else {
        return { location: { $in: ['New York', 'San Francisco', 'London', 'Tokyo'] } };
      }
    },
    
    // Manager status - roughly 20% are managers
    () => {
      if (targetPercentage <= 0.2) {
        return { isManager: true };
      } else {
        return { isManager: false };
      }
    },
    
    // Join date range - distribute across 5 years
    () => {
      const yearsRange = Math.ceil(targetPercentage * 5);
      const endDate = new Date(Date.now() - getRandomInt(0, (5 - yearsRange) * 365) * 24 * 60 * 60 * 1000);
      const startDate = new Date(endDate.getTime() - yearsRange * 365 * 24 * 60 * 60 * 1000);
      return { 
        joinDate: { 
          $gte: startDate,
          $lte: endDate
        } 
      };
    },
    
    // Complex combinations for larger target counts
    () => {
      if (targetPercentage <= 0.3) {
        const ageRange = Math.ceil(targetPercentage * 30) + 15;
        const minAge = getRandomInt(20, 60);
        return {
          $and: [
            { age: { $gte: minAge, $lte: minAge + ageRange } },
            { status: { $in: ['active', 'pending'] } },
            { salary: { $gte: 40000 } }
          ]
        };
      } else {
        return {
          $and: [
            { age: { $gte: 25, $lte: 65 } },
            { status: { $in: ['active', 'pending', 'inactive'] } },
            { salary: { $gte: 35000 } }
          ]
        };
      }
    },
    
    () => {
      if (targetPercentage <= 0.4) {
        return {
          $or: [
            { department: 'engineering' },
            { $and: [{ isManager: true }, { salary: { $gte: 70000 } }] }
          ]
        };
      } else {
        return {
          $or: [
            { department: { $in: ['engineering', 'marketing'] } },
            { salary: { $gte: 60000 } }
          ]
        };
      }
    },

    // Additional targeted filters for specific ranges
    () => {
      const ageMin = getRandomInt(25, 45);
      const ageMax = ageMin + Math.ceil(targetPercentage * 40);
      return { 
        $and: [
          { age: { $gte: ageMin, $lte: Math.min(ageMax, 80) } },
          { location: { $in: ['New York', 'San Francisco'] } }
        ]
      };
    },

    () => {
      if (targetPercentage <= 0.5) {
        return {
          $or: [
            { status: 'active' },
            { salary: { $gte: 80000 } }
          ]
        };
      } else {
        return {
          $or: [
            { status: { $in: ['active', 'pending'] } },
            { salary: { $gte: 50000 } }
          ]
        };
      }
    }
  ];
  
  const selectedFilter = filterOptions[getRandomInt(0, filterOptions.length - 1)];
  if (selectedFilter) {
    return selectedFilter();
  }
  // Fallback filter
  return { age: { $gte: 20, $lte: 60 } };
}

// Test the filter to ensure it returns the right amount of users
export async function testUserFilter(db: Db, targetCount: number = 5000): Promise<{ filter: Record<string, any>, count: number, targetCount: number }> {
  const usersCollection = db.collection('users');
  let filter: Record<string, any>;
  let count: number;
  
  // Try different filters until we find one in the right range
  let attempts = 0;
  const minCount = targetCount;
  const maxCount = targetCount + 100;
  
  do {
    filter = generateUserFilter(targetCount);
    count = await usersCollection.countDocuments(filter);
    attempts++;
  } while ((count < minCount || count > maxCount) && attempts < 100);
  
  console.log(`Filter: ${JSON.stringify(filter)}`);
  console.log(`Target: ${targetCount}, Actual: ${count} users (attempts: ${attempts})`);
  
  return { filter, count, targetCount };
}

// Generate multiple test filters with different target counts
export async function generateTestFilters(db: Db, targetCounts: number[] = [1000, 2500, 5000, 7500, 10000]): Promise<Array<{ filter: Record<string, any>, count: number, targetCount: number }>> {
  const filters = [];
  
  console.log(`\nGenerating ${targetCounts.length} test filters...`);
  
  for (let i = 0; i < targetCounts.length; i++) {
    const targetCount = targetCounts[i];
    if (targetCount !== undefined) {
      console.log(`\nTesting filter for ${targetCount} users:`);
      const result = await testUserFilter(db, targetCount);
      filters.push(result);
      
      const withinRange = result.count >= targetCount && result.count <= targetCount + 100;
      console.log(`✓ Filter ${i + 1}: ${result.count} users ${withinRange ? '(✓ within range)' : '(⚠ outside range)'}`);
    }
  }
  
  return filters;
}