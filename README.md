# mongodb-query-performance

Testing the performance of MongoDB queries where documents in one collection must be filtered by filters on referenced documents in another collection.

## Collections

- `users` - Contains user documents with a random amount of properties/values.
- `documents` - Contains documents that reference users via the `userId` property and have their own properties/values.

## Queries

1. Find query with `$in` operator: `.find` query using `$in` to filter documents based on user IDs.
2. Aggregation pipeline with `$in` operator: `.aggregate` pipeline using `$in` to filter documents based on user IDs.
3. Aggregation pipeline with `$lookup`: `.aggregate` pipeline using `$lookup` to join `documents` with `users` and filter based on user properties.
4. (Skip/Limit Paginated) Find query with `$in` operator: Paginated `.find` query using `$in` to filter documents based on user IDs. Uses a second `.countDocuments` query to get total count.
5. (Skip/Limit Paginated) Aggregation pipeline with `$in` operator: Paginated `.aggregate` pipeline using `$in` to filter documents based on user IDs. Uses a `$facet` stage to get total count.
6. (Skip/Limit Paginated) Aggregation pipeline with `$lookup`: Paginated `.aggregate` pipeline using `$lookup` to join `documents` with `users` and filter based on user properties. Uses a `$facet` stage to get total count.

## Usage

1. Ensure you have a running instance of MongoDB.
2. Run `index.ts` with the `--seed` flag:

```bash
bun run ./index.ts --seed
```

> NOTE: This delete all existing seeded data and re-seeds the database. THIS WILL TAKE A WHILE.

Optionally, you can just re-seed the filters (`filters.json`) with the `--seed-filters` flag:

```bash
bun run ./index.ts --seed-filters
```

> NOTE: Useful if you want to test queries on your own with user filters for specified number of users.

Running the script without any flags will run based on assumed existing data (and `filters.json`):

```bash
bun run ./index.ts
```
