import { getConnection } from "./db";
import { login, RetsSession } from "../auth";
import { getResources, getTableMetadata, getClasses } from "./tables/metadata";
import { RowDataPacket } from "mysql2";
import fs from "fs";
import path from "path";

interface TranslationRow extends RowDataPacket {
  system_name: string;
  vanity_name: string;
}

interface LockoutEntry {
  resourceId: string;
  className: string;
  lockedUntil: string;
}

// Create the translations table if it doesn't exist
export async function createTranslationsTable(): Promise<void> {
  const connection = await getConnection();
  try {
    await connection.query(`
      CREATE TABLE IF NOT EXISTS \`rets_data\`.\`field_name_translations\` (
        \`id\` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        \`resource_id\` VARCHAR(50) NOT NULL,
        \`class_name\` VARCHAR(50) NOT NULL,
        \`system_name\` VARCHAR(100) NOT NULL,
        \`vanity_name\` VARCHAR(100) NOT NULL,
        \`created_at\` DATETIME DEFAULT CURRENT_TIMESTAMP,
        \`updated_at\` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY \`idx_resource_class_system\` (\`resource_id\`, \`class_name\`, \`system_name\`),
        UNIQUE KEY \`idx_resource_class_vanity\` (\`resource_id\`, \`class_name\`, \`vanity_name\`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    `);
    console.log(
      "Table field_name_translations created successfully in rets_data database"
    );
  } catch (error) {
    console.error(
      "Error creating table:",
      error instanceof Error ? error.message : String(error)
    );
    throw error;
  }
}

// Helper function to make alphanumeric names
function makeAlphanumeric(name: string): string {
  // Remove "Number of" or "NumberOf" prefix if present
  if (name.startsWith("Number of ")) {
    name = name.substring(10); // Remove "Number of " prefix
  } else if (name.startsWith("NumberOf")) {
    name = name.substring(8); // Remove "NumberOf" prefix
  }
  // Remove any non-alphanumeric characters without changing case
  name = name.replace(/[^a-zA-Z0-9]/g, "");
  // Remove "of" prefix if present
  if (name.startsWith("of")) {
    name = name.substring(2); // Remove "of" prefix
  }
  return name;
}

// Populate the translations table with all field mappings
export async function populateFieldTranslations(): Promise<void> {
  const session = await login();
  const resources = await getResources(session);
  const connection = await getConnection();

  // Read lockout list
  const lockoutPath = path.join(process.cwd(), "cache", "rets_lockout.json");
  const lockoutList: LockoutEntry[] = fs.existsSync(lockoutPath)
    ? JSON.parse(fs.readFileSync(lockoutPath, "utf-8"))
    : [];

  console.log("Starting field translation population...");
  let totalFields = 0;
  let skippedResources = 0;

  for (const resource of resources) {
    // Check if resource is locked
    const isLocked = lockoutList.some(
      (entry) => entry.resourceId === resource.ResourceID
    );

    if (isLocked) {
      console.log(`Skipping locked resource: ${resource.ResourceID}`);
      skippedResources++;
      continue;
    }

    console.log(`\nProcessing resource: ${resource.ResourceID}`);
    const classes = await getClasses(session, resource.ResourceID);

    for (const cls of classes) {
      // Check if specific class is locked
      const isClassLocked = lockoutList.some(
        (entry) =>
          entry.resourceId === resource.ResourceID &&
          entry.className === cls.ClassName
      );

      if (isClassLocked) {
        console.log(`Skipping locked class: ${cls.ClassName}`);
        continue;
      }

      console.log(`Processing class: ${cls.ClassName}`);
      const tableMetadata = await getTableMetadata(
        session,
        resource.ResourceID,
        cls.ClassName
      );

      for (const field of tableMetadata) {
        const vanityName = makeAlphanumeric(
          field.LongName || field.StandardName || field.SystemName
        );

        await connection.query(
          `INSERT INTO field_name_translations 
           (resource_id, class_name, system_name, vanity_name) 
           VALUES (?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE vanity_name = VALUES(vanity_name)`,
          [resource.ResourceID, cls.ClassName, field.SystemName, vanityName]
        );
        totalFields++;
      }
    }
  }

  console.log(
    `\nCompleted field translation population. Processed ${totalFields} fields. Skipped ${skippedResources} locked resources.`
  );
}

// Get vanity name for a system name
export async function getVanityName(
  resourceId: string,
  className: string,
  systemName: string
): Promise<string> {
  const connection = await getConnection();
  const [rows] = await connection.query<TranslationRow[]>(
    `SELECT vanity_name FROM field_name_translations 
     WHERE resource_id = ? AND class_name = ? AND system_name = ?`,
    [resourceId, className, systemName]
  );
  return rows[0]?.vanity_name || systemName;
}

// Get system name for a vanity name
export async function getSystemName(
  resourceId: string,
  className: string,
  vanityName: string
): Promise<string> {
  const connection = await getConnection();
  const [rows] = await connection.query<TranslationRow[]>(
    `SELECT system_name FROM field_name_translations 
     WHERE resource_id = ? AND class_name = ? AND vanity_name = ?`,
    [resourceId, className, vanityName]
  );
  return rows[0]?.system_name || vanityName;
}

// Translate a record between system and vanity names
export async function translateRecord(
  resourceId: string,
  className: string,
  record: Record<string, any>,
  direction: "toVanity" | "toSystem"
): Promise<Record<string, any>> {
  const connection = await getConnection();
  const [rows] = await connection.query<TranslationRow[]>(
    `SELECT system_name, vanity_name FROM field_name_translations 
     WHERE resource_id = ? AND class_name = ?`,
    [resourceId, className]
  );

  const translations = rows.reduce((acc, row) => {
    acc[row.system_name] = row.vanity_name;
    return acc;
  }, {} as Record<string, string>);

  const result: Record<string, any> = {};
  for (const [key, value] of Object.entries(record)) {
    if (direction === "toVanity") {
      result[translations[key] || key] = value;
    } else {
      const systemName =
        Object.entries(translations).find(([_, v]) => v === key)?.[0] || key;
      result[systemName] = value;
    }
  }
  return result;
}

// Get all translations for a resource/class
export async function getTranslationsForResource(
  resourceId: string,
  className: string
): Promise<Record<string, string>> {
  const connection = await getConnection();
  const [rows] = await connection.query<TranslationRow[]>(
    `SELECT system_name, vanity_name FROM field_name_translations 
     WHERE resource_id = ? AND class_name = ?`,
    [resourceId, className]
  );

  return rows.reduce((acc, row) => {
    acc[row.system_name] = row.vanity_name;
    return acc;
  }, {} as Record<string, string>);
}
