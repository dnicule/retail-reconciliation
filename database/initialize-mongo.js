/**
 * @file Initialize a fresh retail_catalog.products collection using mongosh inside MongoDB.
 * Matches the application validator, refuses existing collections, and never inserts sample data.
 */
const catalog = db.getSiblingDB('retail_catalog');
const validator = {
  $jsonSchema: {
    bsonType: 'object',
    required: ['_id', 'name', 'description', 'category', 'sourceRevision', 'appliedAt'],
    additionalProperties: true,
    properties: {
      _id: {
        bsonType: 'string',
        minLength: 1,
        maxLength: 64,
        pattern: '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$',
        // Exclude a final newline as well as other non-identifier characters.
        not: { pattern: '[^A-Za-z0-9._-]' },
      },
      name: { bsonType: 'string', minLength: 1, maxLength: 200 },
      description: { bsonType: 'string', minLength: 0, maxLength: 4000 },
      category: { bsonType: 'string', minLength: 1, maxLength: 100 },
      sourceRevision: { bsonType: 'int', minimum: 1, maximum: 2147483647 },
      appliedAt: { bsonType: 'date' },
      seoTitle: { bsonType: ['string', 'null'], maxLength: 200 },
    },
  },
};

try {
  const existing = catalog.runCommand({
    listCollections: 1,
    filter: { name: 'products' },
    cursor: { batchSize: 1 },
    maxTimeMS: 5000,
  });
  if (existing.ok !== 1) {
    throw new Error('Cannot inspect MongoDB collections; no schema changes requested.');
  }
  if (existing.cursor.firstBatch.length !== 0) {
    throw new Error('retail_catalog.products already exists; leaving its schema and data unchanged.');
  }

  // Create the validator with the collection, so no unvalidated collection is exposed to writers.
  const created = catalog.runCommand({
    create: 'products',
    validator,
    validationLevel: 'strict',
    validationAction: 'error',
    writeConcern: { w: 1, wtimeout: 5000 },
    maxTimeMS: 10000,
  });
  if (created.ok !== 1 || created.writeConcernError) {
    throw new Error('MongoDB creation was not confirmed. Inspect the collection before retrying.');
  }
  print('[database] Created retail_catalog.products with strict validation. No products were inserted.');
} catch (error) {
  console.error(`[database] ${error.message}`);
  exit(1);
}
