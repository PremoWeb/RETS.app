/**
 * syncRetsData.ts
 *
 * Synchronizes RETS (Real Estate Transaction Standard) data with local database.
 * Handles table creation, updates, and unauthorized access management.
 * Supports both full and partial syncs based on resource and update fields.
 * Runs as a scheduled service every minute.
 */

import * as fs from "fs/promises";
import * as path from "path";
import { CronJob } from "cron";
import { login, createAuthenticatedOptions, RetsSession } from "./lib/auth";
import {
  getTableMetadata,
  getClasses,
  generateCreateTableSQL,
} from "./lib/db/tables";
import { getUpdateFields } from "./lib/rets/updateFields";
import { RetsParser } from "./lib/rets/retsParser";
import { makeRetsRequest } from "./lib/utils/http";
import {
  getConnection,
  tableExists,
  getLatestUpdateValue,
  buildUpsertSQL,
  sanitizeRecord,
  formatRetsDate,
} from "./lib/db/db";
import { startPhotoServer } from "./lib/rets/photoServer";
import { photoProcessorService } from "./lib/rets/photoProcessingService";
import { checkPropertyStatus } from "./lib/rets/propertyCleanupService";

// Ensure cache directory exists
const CACHE_DIR = path.join(process.cwd(), "cache");
await fs.mkdir(CACHE_DIR, { recursive: true });

const LOCKOUT_FILE = path.join(CACHE_DIR, "rets_lockout.json");

// Track last full sync time
let lastFullSync: Date | null = null;

// Check if it's time for a full sync
function shouldRunFullSync(): boolean {
  if (!lastFullSync) return true;

  const now = new Date();
  const hoursSinceLastSync =
    (now.getTime() - lastFullSync.getTime()) / (1000 * 60 * 60);
  return hoursSinceLastSync >= 3;
}

async function loadLockoutSet(): Promise<Set<string>> {
  try {
    const data = await fs.readFile(LOCKOUT_FILE, "utf8");
    const arr = JSON.parse(data);
    return new Set(arr);
  } catch {
    return new Set();
  }
}

async function saveLockoutSet(lockout: Set<string>) {
  await fs.writeFile(
    LOCKOUT_FILE,
    JSON.stringify(Array.from(lockout), null, 2),
    "utf8"
  );
}

async function fetchRetsUpdates(
  session: RetsSession,
  resource: string,
  updateField: string,
  lastValue: string | null,
  upsertCallback: (records: any[], parsed: any) => Promise<void>,
  className: string | null = null
): Promise<void> {
  const searchUrl = session.capabilityUrls.Search;
  if (!searchUrl) throw new Error("Search URL not found in capability URLs");
  let SearchType: string;
  let Class: string;
  if (className) {
    SearchType = resource;
    Class = className;
  } else {
    const underscoreIndex = resource.indexOf("_");
    if (underscoreIndex !== -1) {
      SearchType = resource.substring(0, underscoreIndex);
      Class = resource.substring(underscoreIndex + 1);
    } else {
      SearchType = resource;
      Class = resource;
    }
  }
  let classList: any[] = [];
  try {
    classList = await getClasses(session, SearchType);
  } catch (e) {
    // ignore, fallback to default
  }
  if (classList.length === 1 && classList[0].ClassName === SearchType) {
    console.log(
      `[ADJUST] Resource ${resource}: Only one class matching resource name, setting Class = resource name ('${SearchType}') for DMQL2 query.`
    );
    Class = SearchType;
  }
  if (updateField === "N/A") {
    console.log(`[SYNC MODE] ${resource}: FULL SYNC (no update field)`);
  } else {
    console.log(
      `[SYNC MODE] ${resource}: PARTIAL SYNC using update field '${updateField}'`
    );
  }
  const batchSize = 2500;
  let offset = 1;
  let hasMore = true;
  while (hasMore) {
    let queryParams: URLSearchParams;
    if (updateField === "N/A") {
      // Full sync: no Query parameter
      queryParams = new URLSearchParams({
        SearchType,
        Class,
        QueryType: "DMQL2",
        Format: "COMPACT",
        StandardNames: "0",
        Count: "1",
        Limit: batchSize.toString(),
        Offset: offset.toString(),
      });
    } else {
      let query = "";
      const formattedLastValue = formatRetsDate(lastValue);
      if (formattedLastValue) {
        query = `(${updateField}=${formattedLastValue}+)`;
      } else {
        query = `(${updateField}=1900-01-01T00:00:00+)`;
      }
      queryParams = new URLSearchParams({
        SearchType,
        Class,
        QueryType: "DMQL2",
        Format: "COMPACT",
        StandardNames: "0",
        Query: query,
        Count: "1",
        Limit: batchSize.toString(),
        Offset: offset.toString(),
      });
    }
    const options = createAuthenticatedOptions(session, searchUrl, queryParams);
    const { response } = await makeRetsRequest(options);
    const parsed = await RetsParser.parse(response.toString());
    const records = parsed.records || [];
    await upsertCallback(records, parsed);
    if (records.length < batchSize) {
      hasMore = false;
    } else {
      offset += batchSize;
    }
  }
}

async function syncData() {
  const connection = await getConnection();
  let session: RetsSession | null = null;
  const lockout = await loadLockoutSet();
  const skippedCombinations = new Map<string, Set<string>>();
  let tablesCreated = false;

  try {
    session = await login();
    const updateFields = await getUpdateFields();

    // First, collect all skipped resource/class combinations
    for (const entry of updateFields) {
      const { resource, classes } = entry;
      const classList = classes && classes.length > 0 ? classes : [null];
      for (const className of classList) {
        const lockoutKey = `${resource}::${className}`;
        if (lockout.has(lockoutKey)) {
          if (!skippedCombinations.has(resource)) {
            skippedCombinations.set(resource, new Set());
          }
          skippedCombinations.get(resource)!.add(className || "default");
        }
      }
    }

    // Display skipped combinations if any
    if (skippedCombinations.size > 0) {
      console.log("\n[LOCKOUT] Unauthorized access:");
      for (const [resource, classes] of skippedCombinations) {
        const classList = Array.from(classes).map((c) =>
          c === "default" ? resource : c
        );
        console.log(`  ${resource}: ${classList.join(", ")}`);
      }
    }

    // Process resources
    for (const entry of updateFields) {
      const { resource, updateField, classes, syncType } = entry;
      const classList = classes && classes.length > 0 ? classes : [null];
      for (const className of classList) {
        let table: string;
        if (resource === "Deleted") {
          table = `Deleted_${className}`;
        } else if (
          classList.length === 1 &&
          (className === resource || className === null)
        ) {
          table = resource;
        } else {
          table = `${resource}_${className}`;
        }
        const lockoutKey = `${resource}::${className}`;
        if (lockout.has(lockoutKey)) {
          continue;
        }
        // Check if table exists, if not, create it
        let tableMetadata;
        let clsMeta = null;
        if (!(await tableExists(connection, table))) {
          tablesCreated = true;
          if (resource === "Deleted") {
            tableMetadata = await getTableMetadata(
              session,
              resource,
              className
            );
          } else {
            tableMetadata = await getTableMetadata(
              session,
              resource,
              className === null ? resource : className
            );
            if (className && className !== resource) {
              const classes = await getClasses(session, resource);
              clsMeta = classes.find((c: any) => c.ClassName === className);
            }
          }
          const createSQL = generateCreateTableSQL(
            table,
            tableMetadata,
            entry,
            clsMeta
          );
          console.log(`[CREATE] Creating table: ${table}`);
          await connection.query(createSQL);
        } else {
          // If table exists, fetch metadata for upsert
          if (resource === "Deleted") {
            tableMetadata = await getTableMetadata(
              session,
              resource,
              className
            );
          } else {
            tableMetadata = await getTableMetadata(
              session,
              resource,
              className === null ? resource : className
            );
            if (className && className !== resource) {
              const classes = await getClasses(session, resource);
              clsMeta = classes.find((c: any) => c.ClassName === className);
            }
          }
        }
        let lastValue: string | null = null;
        if (updateField !== "N/A") {
          lastValue = await getLatestUpdateValue(
            connection,
            table,
            updateField
          );
          const formattedLastValue = formatRetsDate(lastValue);
          console.log(
            `[SYNC] Processing table: ${table}, using date: ${
              formattedLastValue ?? "1900-01-01T00:00:00"
            }`
          );
        } else {
          // Only perform full sync at scheduled intervals
          if (!shouldRunFullSync()) {
            console.log(
              `[SKIP] Skipping full sync for ${table} (not scheduled)`
            );
            continue;
          }
          console.log(
            `[SYNC] Processing table: ${table} (full sync, no update field)`
          );
          console.log(`[FULL SYNC] Truncating table: ${table}`);
          await connection.query(`TRUNCATE TABLE \`${table}\``);
        }
        let upserted = 0;
        let unauthorized = false;
        await fetchRetsUpdates(
          session,
          resource,
          updateField,
          lastValue,
          async (records, parsed) => {
            if (records.length === 0) {
              const unauthorizedInfo = RetsParser.isUnauthorizedQuery(parsed);
              if (unauthorizedInfo) {
                console.warn(
                  `[LOCKOUT] Unauthorized access detected for resource/class: ${resource} / ${className}. Dropping table ${table} and locking out future sync attempts.`
                );
                lockout.add(lockoutKey);
                await saveLockoutSet(lockout);
                await connection.query(`DROP TABLE IF EXISTS ${table}`);
                unauthorized = true;
                return;
              }
            }
            if (unauthorized) return;
            for (const record of records) {
              const sanitizedRecord = sanitizeRecord(record, tableMetadata);
              const { sql, values } = buildUpsertSQL(table, sanitizedRecord);
              try {
                await connection.query(sql, values);
                upserted++;
              } catch (err) {
                const errorMsg =
                  err instanceof Error ? err.message : String(err);
                const match = errorMsg.match(/column '([^']+)'/);
                if (match) {
                  const field = match[1];
                  const value = sanitizedRecord[field];
                  console.error(
                    `\n[ERROR] Failed to upsert record into ${table}: ${errorMsg}`
                  );
                  console.error(
                    `[ERROR] Field: ${field}, Value: ${JSON.stringify(value)}`
                  );
                } else {
                  console.error(
                    `\n[ERROR] Failed to upsert record into ${table}:`,
                    errorMsg
                  );
                }
              }
            }
          },
          className
        );
        console.log(`Upserted ${upserted} records into ${table}`);
      }
    }

    // Update last full sync time if we performed any full syncs
    if (shouldRunFullSync()) {
      lastFullSync = new Date();
    }

    if (tablesCreated) {
      console.log("Auto table creation complete.");
    }
  } catch (error) {
    console.error(
      "Auto sync error:",
      error instanceof Error ? error.message : String(error)
    );
  } finally {
    await connection.release();
  }
}

// Start the main service
async function startService() {
  try {
    // Start the photo server
    const photoServer = startPhotoServer();

    // Start the photo processor service
    photoProcessorService().catch((error) => {
      console.error("Error in photo processor service:", error);
    });

    // Start the data sync cron job
    const syncJob = new CronJob("*/1 * * * *", async () => {
      try {
        await syncData();
      } catch (error) {
        console.error("Error in sync job:", error);
      }
    });

    // Start the property cleanup job (every 3 hours from noon to midnight)
    const cleanupJob = new CronJob("0 12,15,18,21,0 * * *", async () => {
      try {
        console.log("\n[PROPERTY CLEANUP] Starting scheduled cleanup...");
        await checkPropertyStatus();
        console.log("[PROPERTY CLEANUP] Cleanup completed successfully");
      } catch (error) {
        console.error("[PROPERTY CLEANUP] Error in cleanup job:", error);
      }
    });

    syncJob.start();
    cleanupJob.start();
    console.log("Data sync job started");
    console.log(
      "Property cleanup job started (runs at 12:00, 15:00, 18:00, 21:00, 00:00)"
    );

    // Keep the process running
    process.on("SIGINT", () => {
      console.log("Shutting down...");
      syncJob.stop();
      cleanupJob.stop();
      photoServer.stop();
      process.exit(0);
    });
  } catch (error) {
    console.error("Error starting service:", error);
    process.exit(1);
  }
}

// Start the service
startService().catch(console.error);
