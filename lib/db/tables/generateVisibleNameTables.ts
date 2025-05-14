import * as fs from "fs/promises";
import * as path from "path";
import { login, RetsSession } from "../../auth";
import { getResources, getTableMetadata, getClasses } from "./metadata";
import { getRetsToMySQLType } from "./schema";

async function loadLockoutSet(): Promise<Set<string>> {
  try {
    const data = await fs.readFile(
      path.join(process.cwd(), "cache", "rets_lockout.json"),
      "utf8"
    );
    const arr = JSON.parse(data);
    return new Set(arr);
  } catch {
    return new Set();
  }
}

export async function generateVisibleNameTables(): Promise<void> {
  const sqlDir = path.join(process.cwd(), "sql", "visible_name_tables");
  console.log(`Creating directory: ${sqlDir}`);

  // Create sql directory if it doesn't exist
  await fs.mkdir(sqlDir, { recursive: true });

  // Get existing session
  const session = await login();

  // Load lockout set
  const lockout = await loadLockoutSet();
  console.log(`Loaded ${lockout.size} lockout entries`);

  // Fetch all resources
  console.log("Fetching resources...");
  const resources = await getResources(session);
  console.log(`Found ${resources.length} total resources`);
  console.log("Resource IDs:", resources.map((r) => r.ResourceID).join(", "));

  // Process all resources
  for (const resource of resources) {
    console.log(`\nProcessing resource: ${resource.ResourceID}`);

    // Fetch classes for this resource
    const classes = await getClasses(session, resource.ResourceID);
    console.log(`Found ${classes.length} classes for ${resource.ResourceID}`);

    // If no classes, use default class "0"
    if (classes.length === 0) {
      const lockoutKey = `${resource.ResourceID}::0`;
      if (lockout.has(lockoutKey)) {
        console.log(`Skipping locked resource: ${resource.ResourceID}`);
        continue;
      }

      console.log(`No classes found, using default class "0"`);
      const tableMetadata = await getTableMetadata(
        session,
        resource.ResourceID,
        "0"
      );
      console.log(`Found ${tableMetadata.length} fields in metadata`);
      await generateVisibleNameTableSQL(resource, null, tableMetadata, sqlDir);
    } else {
      // Generate tables for each class
      for (const cls of classes) {
        const lockoutKey = `${resource.ResourceID}::${cls.ClassName}`;
        if (lockout.has(lockoutKey)) {
          console.log(
            `Skipping locked resource/class: ${resource.ResourceID}/${cls.ClassName}`
          );
          continue;
        }

        console.log(`Processing class: ${cls.ClassName}`);
        const tableMetadata = await getTableMetadata(
          session,
          resource.ResourceID,
          cls.ClassName
        );
        console.log(`Found ${tableMetadata.length} fields in metadata`);
        await generateVisibleNameTableSQL(resource, cls, tableMetadata, sqlDir);
      }
    }
  }
}

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

async function generateVisibleNameTableSQL(
  resource: any,
  cls: any | null,
  tableMetadata: any[],
  sqlDir: string
): Promise<void> {
  // Generate table name without duplicates
  let tableName: string;
  if (cls === null) {
    tableName = `${resource.ResourceID}_visible`;
  } else if (cls.ClassName === resource.ResourceID) {
    tableName = `${resource.ResourceID}_visible`;
  } else {
    tableName = `${resource.ResourceID}_${cls.ClassName}_visible`;
  }

  const sql: string[] = [];
  sql.push(`CREATE TABLE IF NOT EXISTS \`${tableName}\` (`);

  if (!resource.KeyField) {
    sql.push("  `id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,");
  }

  const fields = tableMetadata.map((field: any) => {
    const sqlType = getRetsToMySQLType(field);
    const isRequired = field.Required === "1" ? " NOT NULL" : "";
    const isPrimary =
      field.SystemName === resource.KeyField ? " PRIMARY KEY" : "";
    const longName = field.LongName || field.StandardName || field.SystemName;
    const columnName = makeAlphanumeric(longName);
    return `  \`${columnName}\` ${sqlType}${isRequired}${isPrimary} COMMENT '${longName}'`;
  });

  sql.push(fields.join(",\n"));

  let tableComment = resource.Description;
  if (cls && cls.Description) {
    tableComment += ` - ${cls.Description}`;
  }
  tableComment += " (Visible Names)";

  sql.push(`) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT '${tableComment}';`);

  // Write SQL to file
  const sqlFile = path.join(sqlDir, `${tableName}.sql`);
  console.log(`Writing SQL to file: ${sqlFile}`);
  await fs.writeFile(sqlFile, sql.join("\n"));
  console.log(`Generated SQL for ${tableName}`);
}
