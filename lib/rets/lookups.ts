import { getConnection } from "../db/db";

interface LookupValue {
  short_value: string;
  long_value: string;
  metadata: {
    sort: number;
    active: boolean;
    [key: string]: any;
  };
}

interface ResourceLookups {
  [fieldName: string]: {
    [shortValue: string]: LookupValue;
  };
}

interface ClassLookups {
  [classId: string]: ResourceLookups;
}

interface LookupCache {
  [resourceId: string]: ClassLookups;
}

let lookupCache: LookupCache = {};

export async function loadLookups(): Promise<void> {
  const conn = await getConnection();

  try {
    // Clear existing cache
    lookupCache = {};

    // First, load common Property lookups from the view
    const [propertyRows] = await conn.query(`
            SELECT field_name, short_value, long_value, metadata
            FROM property_common_lookups
            ORDER BY field_name, JSON_EXTRACT(metadata, '$.sort')
        `);

    // Initialize Property in cache
    lookupCache["Property"] = {
      COMMON: {}, // Special class for common fields
    };

    // Build common Property lookups
    for (const row of propertyRows as any[]) {
      if (!lookupCache["Property"]["COMMON"][row.field_name]) {
        lookupCache["Property"]["COMMON"][row.field_name] = {};
      }

      lookupCache["Property"]["COMMON"][row.field_name][row.short_value] = {
        short_value: row.short_value,
        long_value: row.long_value,
        metadata: JSON.parse(row.metadata),
      };
    }

    // Then load all other lookups
    const [rows] = await conn.query(`
            SELECT resource_id, class_id, field_name, short_value, long_value, metadata
            FROM lookup_values
            WHERE resource_id != 'Property' OR field_name NOT IN (
                SELECT field_name FROM property_common_lookups
            )
            ORDER BY resource_id, class_id, field_name, JSON_EXTRACT(metadata, '$.sort')
        `);

    // Build the cache for non-common fields
    for (const row of rows as any[]) {
      if (!lookupCache[row.resource_id]) {
        lookupCache[row.resource_id] = {};
      }
      if (!lookupCache[row.resource_id][row.class_id]) {
        lookupCache[row.resource_id][row.class_id] = {};
      }
      if (!lookupCache[row.resource_id][row.class_id][row.field_name]) {
        lookupCache[row.resource_id][row.class_id][row.field_name] = {};
      }

      lookupCache[row.resource_id][row.class_id][row.field_name][
        row.short_value
      ] = {
        short_value: row.short_value,
        long_value: row.long_value,
        metadata: JSON.parse(row.metadata),
      };
    }

    console.log("Lookup cache loaded successfully");
  } finally {
    conn.end();
  }
}

// Get all lookup values for a specific field
export function getFieldLookups(
  resourceId: string,
  classId: string,
  fieldName: string
): Record<string, LookupValue> | undefined {
  return lookupCache[resourceId]?.[classId]?.[fieldName];
}

// Get a specific lookup value
export function getLookupValue(
  resourceId: string,
  classId: string,
  fieldName: string,
  shortValue: string
): LookupValue | undefined {
  return lookupCache[resourceId]?.[classId]?.[fieldName]?.[shortValue];
}

// Get all lookup values for a resource and class
export function getResourceClassLookups(
  resourceId: string,
  classId?: string
): ResourceLookups | undefined {
  // For Property resource, return common fields if no class specified
  if (resourceId === "Property" && !classId) {
    return lookupCache["Property"]?.["COMMON"];
  }
  return lookupCache[resourceId]?.[classId || "COMMON"];
}

// Example usage:
// const propertyLookups = getResourceClassLookups('Property'); // Gets common fields across all Property classes
// const propertyType = getLookupValue('Property', 'COMMON', 'PropertyType', '1');
// const statusLookups = getFieldLookups('Property', 'COMMON', 'Status');
