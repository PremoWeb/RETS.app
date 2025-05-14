import * as fs from "fs/promises";
import * as path from "path";
import { login, logout, RetsSession } from "../../auth";
import { getResources, getTableMetadata, getClasses } from "./metadata";
import { generateCreateTableSQL } from "./schema";

export async function generateTableSchemas(): Promise<void> {
  let session: RetsSession | null = null;
  const sqlDir = path.join(process.cwd(), "sql");

  try {
    // Create sql directory if it doesn't exist
    await fs.mkdir(sqlDir, { recursive: true });

    // Perform Login
    session = await login();

    // Fetch all resources
    const resources = await getResources(session);

    // Iterate through each resource
    for (const resource of resources) {
      // Fetch classes for this resource
      const classes = await getClasses(session, resource.ResourceID);

      if (classes.length === 0) {
        // If no classes, create a single file for the resource
        const tableMetadata = await getTableMetadata(
          session,
          resource.ResourceID,
          "0"
        );
        const sql = generateCreateTableSQL(
          resource.ResourceID,
          tableMetadata,
          resource
        );

        // Save resource SQL to file (without class name)
        const filename = path.join(sqlDir, `${resource.ResourceID}.sql`);
        await fs.writeFile(filename, sql, "utf8");
      } else {
        // If has classes, create a file for each class
        for (const cls of classes) {
          const tableName =
            cls.ClassName === resource.ResourceID
              ? resource.ResourceID
              : `${resource.ResourceID}_${cls.ClassName}`;
          const tableMetadata = await getTableMetadata(
            session,
            resource.ResourceID,
            cls.ClassName
          );
          const sql = generateCreateTableSQL(
            tableName,
            tableMetadata,
            resource,
            cls
          );

          // Save resource SQL to file
          const filename =
            cls.ClassName === resource.ResourceID
              ? path.join(sqlDir, `${resource.ResourceID}.sql`)
              : path.join(
                  sqlDir,
                  `${resource.ResourceID}_${cls.ClassName}.sql`
                );
          await fs.writeFile(filename, sql, "utf8");
        }
      }
    }
  } catch (error) {
    console.error(
      "Error:",
      error instanceof Error ? error.message : String(error)
    );
    process.exit(1);
  } finally {
    // Perform Logout in CLI mode
    if (session) {
      try {
        await logout(session);
      } catch (logoutError) {
        console.error(
          "Logout failed:",
          logoutError instanceof Error
            ? logoutError.message
            : String(logoutError)
        );
      }
    }
  }
}
