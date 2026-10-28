/**
 * @file Defines MongoDB schema expectations and read-only metadata commands for worker readiness.
 * Pure builders return fresh command objects; execution, deadlines, and validation remain in the adapter.
 */
import { productCollectionName } from './products.js';

/** Existing collection contract, independently represented for read-only comparison. */
export const mongoValidator = {
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

/** Existing collection options required for compatible product writes. */
export const expectedProductCollection = {
  type: 'collection',
  validationLevel: 'strict',
  validationAction: 'error',
};

/** Built-in identifier index expected on the existing products collection. */
export const expectedProductIndex = { name: '_id_', key: { _id: 1 } };

/** Effective permissions required for product operations and index inspection. */
export const requiredProductActions = ['find', 'insert', 'update', 'listIndexes'] as const;

/**
 * Request the existing product collection's metadata without enumerating product records.
 * @returns A new listCollections command limited to the catalog collection and a two-entry batch.
 */
export function productCollectionMetadataCommand(): {
  listCollections: 1;
  filter: { name: typeof productCollectionName };
  cursor: { batchSize: number };
} {
  return { listCollections: 1, filter: { name: productCollectionName }, cursor: { batchSize: 2 } };
}

/**
 * Request the existing product indexes without creating or modifying any index.
 * @returns A new listIndexes command with the existing bounded metadata batch size.
 */
export function productIndexMetadataCommand(): {
  listIndexes: typeof productCollectionName;
  cursor: { batchSize: number };
} {
  return { listIndexes: productCollectionName, cursor: { batchSize: 100 } };
}

/**
 * Request current authenticated privileges without changing users or roles.
 * @returns A new read-only connectionStatus command requesting effective privileges.
 */
export function connectionPrivilegesCommand(): { connectionStatus: 1; showPrivileges: true } {
  return { connectionStatus: 1, showPrivileges: true };
}
