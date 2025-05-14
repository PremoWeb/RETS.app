import { RetsParser } from "./lib/rets/retsParser";
import {
  login,
  logout,
  createAuthenticatedOptions,
  RetsSession,
} from "./lib/auth/auth";
import { getConnection } from "./lib/db/db";
import * as fs from "fs/promises";
import * as path from "path";
import { makeRetsRequest } from "./lib/utils/http";

interface LookupValue {
  Value: string;
  LongValue: string;
}

interface LookupData {
  [resourceId: string]: {
    [classId: string]: {
      [lookupName: string]: LookupValue[];
    };
  };
}

// Fetch METADATA-RESOURCE
async function getResources(session: RetsSession): Promise<any[]> {
  console.log("Fetching resources...");
  const metadataUrl = session.capabilityUrls.GetMetadata;
  const queryParams = new URLSearchParams({
    Type: "METADATA-RESOURCE",
    Format: "COMPACT",
    ID: "0",
  });
  const options = createAuthenticatedOptions(session, metadataUrl, queryParams);
  const { response } = await makeRetsRequest(options);
  const parsed = await RetsParser.parse(response.toString());
  const resources = parsed.Metadata?.Data || [];
  console.log(`Found ${resources.length} resources`);
  return resources;
}

// Fetch METADATA-CLASS
async function getClasses(
  session: RetsSession,
  resourceId: string
): Promise<any[]> {
  console.log(`Fetching classes for resource ${resourceId}...`);
  const metadataUrl = session.capabilityUrls.GetMetadata;
  const queryParams = new URLSearchParams({
    Type: "METADATA-CLASS",
    Format: "COMPACT",
    ID: `${resourceId}:0`,
  });
  const options = createAuthenticatedOptions(session, metadataUrl, queryParams);
  const { response } = await makeRetsRequest(options);
  const parsed = await RetsParser.parse(response.toString());
  const classes = parsed.Metadata?.Data || [];
  console.log(`Found ${classes.length} classes for resource ${resourceId}`);
  return classes;
}

// Fetch METADATA-TABLE
async function getTableMetadata(
  session: RetsSession,
  resourceId: string,
  classId: string
): Promise<any[]> {
  console.log(`Fetching table metadata for ${resourceId}:${classId}...`);
  const metadataUrl = session.capabilityUrls.GetMetadata;
  const queryParams = new URLSearchParams({
    Type: "METADATA-TABLE",
    Format: "COMPACT",
    ID: `${resourceId}:${classId}`,
  });
  const options = createAuthenticatedOptions(session, metadataUrl, queryParams);
  const { response } = await makeRetsRequest(options);
  const parsed = await RetsParser.parse(response.toString());
  const tables = parsed.Metadata?.Data || [];
  console.log(`Found ${tables.length} tables for ${resourceId}:${classId}`);
  return tables;
}

// Fetch METADATA-LOOKUP_TYPE
async function getLookupTypes(
  session: RetsSession,
  resourceId: string,
  lookupName: string
): Promise<any[]> {
  console.log(`Fetching lookup types for ${resourceId}:${lookupName}...`);
  const metadataUrl = session.capabilityUrls.GetMetadata;
  const queryParams = new URLSearchParams({
    Type: "METADATA-LOOKUP_TYPE",
    Format: "COMPACT",
    ID: `${resourceId}:${lookupName}`,
  });
  const options = createAuthenticatedOptions(session, metadataUrl, queryParams);
  const { response } = await makeRetsRequest(options);
  const parsed = await RetsParser.parse(response.toString());
  const lookups = parsed.Metadata?.Data || [];
  console.log(
    `Found ${lookups.length} lookup values for ${resourceId}:${lookupName}`
  );
  return lookups;
}

async function createSchema(conn: any) {
  // Drop existing tables and views
  await conn.query("DROP VIEW IF EXISTS property_common_lookups");
  await conn.query("DROP TABLE IF EXISTS lookup_values");

  // Create the lookup values table
  await conn.query(`
        CREATE TABLE lookup_values (
            id INT AUTO_INCREMENT PRIMARY KEY,
            resource_id VARCHAR(50) NOT NULL,
            class_id VARCHAR(50) NOT NULL,
            field_name VARCHAR(100) NOT NULL,
            short_value VARCHAR(50) NOT NULL,
            long_value VARCHAR(255) NOT NULL,
            metadata JSON,
            UNIQUE KEY unique_lookup_value (resource_id, class_id, field_name, short_value),
            KEY idx_resource_class (resource_id, class_id),
            KEY idx_resource_field (resource_id, field_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

  // Create view for common Property fields
  await conn.query(`
        CREATE VIEW property_common_lookups AS
        SELECT 
            field_name,
            short_value,
            long_value,
            metadata,
            COUNT(DISTINCT class_id) as class_count
        FROM lookup_values
        WHERE resource_id = 'Property'
        GROUP BY field_name, short_value, long_value, metadata
        HAVING class_count = (
            SELECT COUNT(DISTINCT class_id) 
            FROM lookup_values 
            WHERE resource_id = 'Property'
        )
    `);

  console.log("Created lookup_values table and property_common_lookups view");
}

async function syncLookups() {
  let session: RetsSession | null = null;
  const conn = await getConnection();

  try {
    // Step 1: Generate lookups
    console.log("Starting lookup generation...");
    session = await login();

    let lookups: LookupData = {};
    const resources = await getResources(session);
    let totalLookups = 0;

    for (const resource of resources) {
      const resourceId = resource.ResourceID;
      console.log(`\nProcessing resource: ${resourceId}`);
      lookups[resourceId] = {};

      const classes = await getClasses(session, resourceId);
      for (const cls of classes) {
        const classId = cls.ClassName;
        console.log(`\nProcessing class: ${classId}`);
        lookups[resourceId][classId] = {};

        const tableMetadata = await getTableMetadata(
          session,
          resourceId,
          classId
        );
        const lookupFields = tableMetadata.filter(
          (field: any) => field.LookupName
        );
        console.log(`Found ${lookupFields.length} lookup fields in ${classId}`);

        // Process all lookup fields for this class
        for (const field of lookupFields) {
          const lookupName = field.LookupName;
          if (lookups[resourceId][classId][lookupName]) {
            console.log(`Skipping duplicate lookup: ${lookupName}`);
            continue;
          }

          try {
            const lookupTypes = await getLookupTypes(
              session,
              resourceId,
              lookupName
            );
            if (lookupTypes.length > 0) {
              lookups[resourceId][classId][lookupName] = lookupTypes.map(
                (lt: any) => ({
                  Value: lt.Value,
                  LongValue: lt.LongValue,
                })
              );
              totalLookups++;
              console.log(
                `Processed lookup ${totalLookups}: ${resourceId}.${classId}.${lookupName} (${lookupTypes.length} values)`
              );
            } else {
              console.warn(
                `No lookup values found for ${resourceId}.${classId}.${lookupName}`
              );
            }
          } catch (error) {
            console.error(
              `Error processing lookup ${resourceId}.${classId}.${lookupName}:`,
              error
            );
          }
        }
      }
    }

    // Step 2: Save to cache
    const outputPath = path.join(process.cwd(), "cache", "lookup_values.json");
    await fs.mkdir(path.dirname(outputPath), { recursive: true });
    await fs.writeFile(outputPath, JSON.stringify(lookups, null, 2), "utf8");
    console.log(`\nSuccessfully processed ${totalLookups} lookups`);
    console.log(`Lookup values saved to ${outputPath}`);

    // Step 3: Populate database
    console.log("\nPopulating database with lookup values...");
    await createSchema(conn);

    // Insert lookup values
    for (const [resourceId, classes] of Object.entries(lookups)) {
      for (const [classId, lookups] of Object.entries(classes)) {
        for (const [fieldName, values] of Object.entries(lookups)) {
          for (const value of values) {
            await conn.query(
              "INSERT INTO lookup_values (resource_id, class_id, field_name, short_value, long_value, metadata) VALUES (?, ?, ?, ?, ?, ?)",
              [
                resourceId,
                classId,
                fieldName,
                value.Value,
                value.LongValue,
                JSON.stringify({ sort: parseInt(value.Value) || 0 }),
              ]
            );
          }
        }
      }
    }

    console.log("Lookup values populated successfully");
  } catch (error) {
    console.error(
      "Error:",
      error instanceof Error ? error.message : String(error)
    );
    process.exit(1);
  } finally {
    if (session) {
      try {
        await logout(session);
      } catch {}
    }
    await conn.end();
  }
}

// Run the sync script
syncLookups().catch((error) => {
  console.error("Failed to sync lookup values:", error);
  process.exit(1);
});
