# mongodb-query-performance

Testing the performance of MongoDB `find` and `aggregate` queries in the following languages (and drivers):

- JavaScript (Native MongoDB driver)
- JavaScript (Mongoose)
- Python (PyMongo)

## Setup

The following collections will be stored in MongoDB:

- `users`

```json
{
    "_id": "ObjectId",
    "group": "ObjectId (groups)",
    "profile": {
        "firstName": "string",
        "lastName": "string",
        "email": "string"
    }
}
```

- `groups`

```json
{
    "_id": "ObjectId",
    "name": "string"
}
```

- `requests`

```json
{
    "_id": "ObjectId",
    "type": "string",
    "status": "string",
    "user": "ObjectId (users)",
    "createdBy": "ObjectId (users)"
}
```

## Execution

The goal of each function/query will be to transform all documents in `requests` into the following format:

```json
{
    "request_id": "string",
    "request_type": "string",
    "request_status": "string",
    "user_id": "string",
    "user_first_name": "string",
    "user_last_name": "string",
    "user_email": "string",
    "user_group_id": "string",
    "user_group_name": "string",
    "created_by_id": "string",
    "created_by_first_name": "string",
    "created_by_last_name": "string",
    "created_by_email": "string"
    "created_by_group_id": "string",
    "created_by_group_name": "string"
}
```

Each language will perform this via both a `find` query and an `aggregate` query.
