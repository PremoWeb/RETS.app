import { getConnection } from "../db/db";
import { login, RetsSession } from "../auth/auth";
import {
  getPropertyPhotos,
  processPropertyPhotos,
  ProcessedPhoto,
} from "./propertyPhotos";
import { RowDataPacket } from "mysql2";
import { Worker } from "worker_threads";

interface ListingRow extends RowDataPacket {
  L_ListingID: string;
  L_StatusCatID: number;
  L_Last_Photo_updt: Date;
  PropertyType: string;
}

interface WorkerEnv {
  listingId: string;
  propertyType: string;
  sessionId: string;
  capabilityUrls: string;
  lastPhotoUpdate: string;
}

// Track processed listings to avoid duplicates
const processedListings = new Set<string>();

// Configuration for processing
const CONFIG = {
  NORMAL: {
    batchSize: 5,
    waitTime: 5000,
    idleWaitTime: 60000,
  },
  AGGRESSIVE: {
    batchSize: 10,
    waitTime: 1000,
    idleWaitTime: 10000,
  },
};

// Worker function to process a single listing's photos
async function processListingPhotos(
  listingId: string,
  propertyType: string,
  session: RetsSession
): Promise<ProcessedPhoto[]> {
  try {
    const photos = await getPropertyPhotos(session, listingId);
    if (photos.length > 0) {
      return await processPropertyPhotos(listingId, propertyType);
    }
    return [];
  } catch (error) {
    console.error(`Error processing photos for listing ${listingId}:`, error);
    throw error;
  }
}

// Check if property tables exist
async function checkPropertyTables(conn: any): Promise<boolean> {
  try {
    const [rows] = await conn.query(`
      SELECT COUNT(*) as count
      FROM information_schema.tables 
      WHERE table_schema = 'rets_data'
      AND table_name IN ('Property_RE_1', 'Property_MF_4', 'Property_CI_3', 'Property_LD_2')
    `);
    return rows[0].count === 4; // All 4 property tables must exist
  } catch (error) {
    console.error("Error checking property tables:", error);
    return false;
  }
}

// Check if required tables exist and create if missing
async function ensureTablesExist(conn: any): Promise<void> {
  try {
    // Check if PhotoProcessing table exists
    const [rows] = await conn.query(`
      SELECT COUNT(*) as count
      FROM information_schema.tables 
      WHERE table_schema = 'rets_data'
      AND table_name = 'PhotoProcessing'
    `);

    if (rows[0].count === 0) {
      console.log("Creating PhotoProcessing table...");
      await conn.query(`
        CREATE TABLE IF NOT EXISTS \`PhotoProcessing\` (
          \`ListingID\` VARCHAR(50) NOT NULL,
          \`PropertyType\` VARCHAR(10) NOT NULL,
          \`Status\` ENUM('processing', 'completed', 'failed') NOT NULL DEFAULT 'processing',
          \`LastProcessed\` DATETIME NOT NULL,
          \`needsReprocessing\` BOOLEAN NOT NULL DEFAULT FALSE,
          \`RetryCount\` INT NOT NULL DEFAULT 0,
          \`ErrorMessage\` TEXT,
          \`PhotoData\` JSON,
          PRIMARY KEY (\`ListingID\`, \`PropertyType\`),
          INDEX \`idx_status\` (\`Status\`),
          INDEX \`idx_last_processed\` (\`LastProcessed\`),
          INDEX \`idx_needs_reprocessing\` (\`needsReprocessing\`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
      `);
      console.log("PhotoProcessing table created successfully");
    }
  } catch (error) {
    console.error("Error ensuring tables exist:", error);
    throw error;
  }
}

// Check if we have a large backlog of photos to process
async function hasBacklog(conn: any): Promise<boolean> {
  const [rows] = await conn.query(`
    SELECT COUNT(*) as count
    FROM (
      SELECT L_ListingID, L_StatusCatID, 'RE_1' as PropertyType FROM Property_RE_1
      UNION ALL
      SELECT L_ListingID, L_StatusCatID, 'MF_4' as PropertyType FROM Property_MF_4
      UNION ALL
      SELECT L_ListingID, L_StatusCatID, 'CI_3' as PropertyType FROM Property_CI_3
      UNION ALL
      SELECT L_ListingID, L_StatusCatID, 'LD_2' as PropertyType FROM Property_LD_2
    ) pl
    LEFT JOIN PhotoProcessing pp ON pp.ListingID = pl.L_ListingID 
      AND pp.PropertyType = pl.PropertyType
    WHERE pl.L_StatusCatID IN (1, 2)
      AND (pp.ListingID IS NULL OR pp.needsReprocessing = TRUE)
  `);
  return rows[0].count > 20; // Consider it a backlog if more than 20 listings need processing
}

export async function photoProcessorService() {
  const conn = await getConnection();

  try {
    // Ensure required tables exist
    await ensureTablesExist(conn);

    // Get RETS session
    console.log("Logging in to RETS server...");
    const session = await login();
    if (!session) {
      throw new Error("Failed to establish RETS session");
    }
    console.log("Successfully logged in to RETS server");

    while (true) {
      try {
        // Check if property tables exist
        const tablesExist = await checkPropertyTables(conn);
        if (!tablesExist) {
          console.log("Property tables not yet created. Waiting 60 seconds...");
          await new Promise((resolve) => setTimeout(resolve, 60000));
          continue;
        }

        // Check if we have a backlog
        const backlog = await hasBacklog(conn);
        const config = backlog ? CONFIG.AGGRESSIVE : CONFIG.NORMAL;

        // Get listings that need photo processing
        const [rows] = await conn.query<ListingRow[]>(
          `
          SELECT 
            pl.L_ListingID,
            pl.L_StatusCatID,
            pl.L_Last_Photo_updt,
            pl.PropertyType
          FROM (
            SELECT L_ListingID, L_StatusCatID, L_Last_Photo_updt, 'RE_1' as PropertyType FROM Property_RE_1
            UNION ALL
            SELECT L_ListingID, L_StatusCatID, L_Last_Photo_updt, 'MF_4' as PropertyType FROM Property_MF_4
            UNION ALL
            SELECT L_ListingID, L_StatusCatID, L_Last_Photo_updt, 'CI_3' as PropertyType FROM Property_CI_3
            UNION ALL
            SELECT L_ListingID, L_StatusCatID, L_Last_Photo_updt, 'LD_2' as PropertyType FROM Property_LD_2
          ) pl
          LEFT JOIN PhotoProcessing pp ON pp.ListingID = pl.L_ListingID 
            AND pp.PropertyType = pl.PropertyType
          WHERE pl.L_StatusCatID IN (1, 2)
            AND (pp.ListingID IS NULL OR pp.needsReprocessing = TRUE)
          ORDER BY 
            CASE 
              WHEN pp.needsReprocessing = TRUE THEN 0
              ELSE 1
            END,
            CASE pl.L_StatusCatID 
              WHEN 1 THEN 1 
              WHEN 2 THEN 2 
            END,
            pl.L_Last_Photo_updt DESC
          LIMIT ?
        `,
          [config.batchSize]
        );

        if (rows.length === 0) {
          console.log(
            `No listings need photo processing. Waiting ${
              config.idleWaitTime / 1000
            } seconds...`
          );
          await new Promise((resolve) =>
            setTimeout(resolve, config.idleWaitTime)
          );
          continue;
        }

        console.log(
          `\n=== Processing ${rows.length} listings in parallel (${
            backlog ? "Aggressive" : "Normal"
          } mode) ===`
        );
        console.log(`Listings to process:`);
        rows.forEach((row, index) => {
          console.log(`${index + 1}. ${row.L_ListingID} (${row.PropertyType})`);
        });
        console.log("");

        // Process listings in parallel
        await Promise.all(
          rows.map(async (row) => {
            // Mark as processing in tracking table
            await conn.query(
              `
              INSERT INTO PhotoProcessing (ListingID, PropertyType, Status, LastProcessed, needsReprocessing)
              VALUES (?, ?, 'processing', NOW(), FALSE)
              ON DUPLICATE KEY UPDATE 
                Status = 'processing',
                LastProcessed = NOW(),
                needsReprocessing = FALSE
            `,
              [row.L_ListingID, row.PropertyType]
            );

            try {
              const photoData = await processListingPhotos(
                row.L_ListingID,
                row.PropertyType,
                session
              );

              // Update with photo data on success
              await conn.query(
                `
                UPDATE PhotoProcessing 
                SET Status = 'completed', 
                    LastProcessed = NOW(),
                    PhotoData = ?,
                    ErrorMessage = NULL
                WHERE ListingID = ? AND PropertyType = ?
              `,
                [JSON.stringify(photoData), row.L_ListingID, row.PropertyType]
              );
            } catch (error) {
              console.error(
                `[${row.L_ListingID}] Error processing photos:`,
                error
              );
              await conn.query(
                `
                UPDATE PhotoProcessing 
                SET Status = 'failed', 
                    LastProcessed = NOW(),
                    RetryCount = RetryCount + 1,
                    ErrorMessage = ?
                WHERE ListingID = ? AND PropertyType = ?
              `,
                [error.message, row.L_ListingID, row.PropertyType]
              );
            }
          })
        );

        // Wait a short time before checking for more listings
        await new Promise((resolve) => setTimeout(resolve, config.waitTime));
      } catch (error) {
        console.error("Error in photo processing loop:", error);
        await new Promise((resolve) => setTimeout(resolve, 30000));
      }
    }
  } catch (error) {
    console.error("Fatal error in photo processor service:", error);
    process.exit(1);
  }
}
