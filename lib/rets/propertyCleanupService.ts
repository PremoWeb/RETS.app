/**
 * clean.ts
 *
 * This script identifies and processes property listings that need to be updated or removed from our database
 * based on their status in the RETS Hotsheets. It handles two main scenarios:
 *
 * 1. SOLD Listings:
 *    - Finds listings marked as SOLD (status 2) in RETS Hotsheets
 *    - Checks if these listings exist in our property tables with a different status
 *    - Updates their status to SOLD
 *
 * 2. WITHDRAWN/EXPIRED Listings:
 *    - Finds listings marked as WITHDRAWN (status 4) or EXPIRED (status 5) in RETS Hotsheets
 *    - Checks if these listings still exist in our property tables as ACTIVE (status 1) or SOLD (status 2)
 *    - Removes these invalid listings
 */

import { getConnection } from "../db/db";
import { createAuthenticatedOptions, RetsSession, login } from "../auth";
import { getUpdateFields } from "../rets/updateFields";
import { RetsParser } from "../rets/retsParser";
import { makeRetsRequest } from "../utils/http";

// Time filter settings
const TIME_RANGE = {
  enabled: true,
  unit: "days" as "days" | "weeks" | "months",
  value: 1, // Look back 1 day
};

// Function to get unique most recent listings
function getUniqueMostRecentListings(records: any[]): any[] {
  const uniqueListings = new Map<string, any>();
  for (const record of records) {
    const listingId = record.L_ListingID;
    const existingRecord = uniqueListings.get(listingId);
    if (!existingRecord || record.L_StatusDate > existingRecord.L_StatusDate) {
      uniqueListings.set(listingId, record);
    }
  }
  return Array.from(uniqueListings.values());
}

// Function to group matches by table
function groupByTable(
  matches: Map<string, { tables: string[]; address?: string; status?: string }>
): Map<string, string[]> {
  const grouped = new Map<string, string[]>();
  for (const [listingId, info] of matches) {
    for (const table of info.tables) {
      if (!grouped.has(table)) {
        grouped.set(table, []);
      }
      grouped.get(table)!.push(listingId);
    }
  }
  return grouped;
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
    return rows[0].count === 4; // All 4 tables must exist
  } catch (error) {
    console.error("Error checking property tables:", error);
    return false;
  }
}

async function checkPropertyStatus() {
  const connection = await getConnection();
  let session: RetsSession | null = null;
  try {
    // Login to RETS server
    session = await login();
    const searchUrl = session.capabilityUrls.Search;
    if (!searchUrl) {
      throw new Error("Search URL not found in capability URLs");
    }

    // Get update fields
    const updateFields = await getUpdateFields();

    // Get Hotsheet classes from update_fields.json
    const hotsheetResource = updateFields.find(
      (entry: any) => entry.resource === "Hotsheet"
    );
    if (!hotsheetResource) {
      throw new Error("No Hotsheet resource found in update_fields.json");
    }
    const hotsheetClasses = hotsheetResource.classes || [];

    // Calculate date range in RETS format: YYYY-MM-DDTHH:mm:ss+
    const now = new Date();
    let startDate = new Date(now);
    let retsDateStr = "";

    if (TIME_RANGE.enabled) {
      switch (TIME_RANGE.unit) {
        case "days":
          startDate.setDate(now.getDate() - TIME_RANGE.value);
          break;
        case "weeks":
          startDate.setDate(now.getDate() - TIME_RANGE.value * 7);
          break;
        case "months":
          startDate.setMonth(now.getMonth() - TIME_RANGE.value);
          break;
      }

      retsDateStr = `${startDate.getFullYear()}-${String(
        startDate.getMonth() + 1
      ).padStart(2, "0")}-${String(startDate.getDate()).padStart(
        2,
        "0"
      )}T00:00:00+`;
    }

    console.log(
      `\nSearching for SOLD and EXPIRED/WITHDRAWN properties in classes: ${hotsheetClasses.join(
        ", "
      )}${TIME_RANGE.enabled ? ` (since ${retsDateStr})` : ""}`
    );

    // Collect all SOLD and EXPIRED/WITHDRAWN listing IDs from hotsheets
    const soldListingIds = new Set<string>();
    const expiredWithdrawnListingIds = new Set<string>();
    const allListings = new Map<string, any>();
    let totalRecordsFound = 0;
    const statusCounts = {
      sold: 0,
      pending: 0,
      withdrawn: 0,
      expired: 0,
    };

    for (const className of hotsheetClasses) {
      // Combined query for SOLD (2), PENDING (3), WITHDRAWN (4), and EXPIRED (5) WITH date limit
      const query = TIME_RANGE.enabled
        ? `(L_StatusCatID=2,3,4,5),(L_StatusDate=${retsDateStr})`
        : `(L_StatusCatID=2,3,4,5)`;

      const queryParams = new URLSearchParams({
        SearchType: "Hotsheet",
        Class: className,
        QueryType: "DMQL2",
        Query: query,
        Format: "COMPACT-DECODED",
        Count: "1",
        Select: "L_ListingID,L_StatusDate,L_Address,L_Status,L_StatusCatID",
      });

      const options = createAuthenticatedOptions(
        session,
        searchUrl,
        queryParams
      );
      const { response } = await makeRetsRequest(options);

      const parsed = await RetsParser.parse(
        typeof response === "string" ? response : response.toString()
      );

      // Get total count from RETS response
      if (parsed.Count) {
        totalRecordsFound += parseInt(parsed.Count, 10);
      }

      let records: any[] = [];
      if (Array.isArray(parsed)) {
        records = parsed;
      } else if (parsed.records) {
        records = parsed.records;
      } else {
      }

      // Deduplicate by L_ListingID, keeping only the most recent L_StatusDate
      const uniqueListings = getUniqueMostRecentListings(records);
      uniqueListings.forEach((record) => {
        allListings.set(record.L_ListingID, record);
        switch (record.L_StatusCatID) {
          case "SOLD":
            statusCounts.sold++;
            soldListingIds.add(record.L_ListingID);
            break;
          case "PENDING":
            statusCounts.pending++;
            break;
          case "WITHDRAWN":
            statusCounts.withdrawn++;
            expiredWithdrawnListingIds.add(record.L_ListingID);
            break;
          case "EXPIRED":
            statusCounts.expired++;
            expiredWithdrawnListingIds.add(record.L_ListingID);
            break;
        }
      });
    }

    console.log(
      `\nFound ${totalRecordsFound} total records in RETS Hotsheets (remote server)`
    );
    console.log("\nStatus breakdown from RETS:");
    console.log(`- SOLD (2): ${statusCounts.sold}`);
    console.log(`- PENDING (3): ${statusCounts.pending}`);
    console.log(`- WITHDRAWN (4): ${statusCounts.withdrawn}`);
    console.log(`- EXPIRED (5): ${statusCounts.expired}`);

    // Report final unique counts after deduplication
    console.log("\nFinal unique counts to process:");
    console.log(`- Unique SOLD listings: ${soldListingIds.size}`);
    console.log(
      `- Unique EXPIRED/WITHDRAWN listings: ${expiredWithdrawnListingIds.size}`
    );

    const timeStr = TIME_RANGE.enabled
      ? `since ${startDate.toLocaleString()}`
      : "all time";
    console.log(
      `\nProcessing ${soldListingIds.size} SOLD listings and ${expiredWithdrawnListingIds.size} EXPIRED/WITHDRAWN listings ${timeStr}`
    );

    // Get property tables from update_fields.json
    const propertyResource = updateFields.find(
      (entry: any) => entry.resource === "Property"
    );
    if (!propertyResource) {
      throw new Error("No Property resource found in update_fields.json");
    }
    const propertyTables = propertyResource.classes.map(
      (cls: string) => `Property_${cls}`
    );

    // Check if property tables exist
    const tablesExist = await checkPropertyTables(connection);
    if (!tablesExist) {
      console.log("Property tables not yet created. Waiting 60 seconds...");
      await new Promise((resolve) => setTimeout(resolve, 60000));
      return;
    }

    // Check all listings in one query per table
    const soldMatches = new Map<
      string,
      { tables: string[]; address?: string; status?: string }
    >();
    const expiredMatches = new Map<
      string,
      { tables: string[]; address?: string; status?: string }
    >();

    // Combine all IDs we need to check
    const allListingIds = new Set([
      ...soldListingIds,
      ...expiredWithdrawnListingIds,
    ]);
    let foundAnyRecords = false;

    if (allListingIds.size > 0) {
      for (const table of propertyTables) {
        const [rows] = await connection.query(
          `SELECT L_ListingID, L_Address, L_StatusCatID FROM ${table} WHERE L_ListingID IN (?)`,
          [Array.from(allListingIds)]
        );

        const matchingRows = (rows as any[]).filter(
          (row) =>
            (soldListingIds.has(row.L_ListingID) &&
              row.L_StatusCatID !== "2") ||
            (expiredWithdrawnListingIds.has(row.L_ListingID) &&
              ["1", "2"].includes(row.L_StatusCatID))
        );

        if (matchingRows.length > 0) {
          foundAnyRecords = true;
          console.log(`\nFound in ${table}:`);
          for (const row of matchingRows) {
            if (
              soldListingIds.has(row.L_ListingID) &&
              row.L_StatusCatID !== "2"
            ) {
              console.log(
                `- ${row.L_ListingID} (${row.L_Address}): Status ${row.L_StatusCatID} -> SOLD`
              );
              const existing = soldMatches.get(row.L_ListingID) || {
                tables: [] as string[],
                address: row.L_Address,
                status: row.L_StatusCatID,
              };
              existing.tables.push(table);
              soldMatches.set(row.L_ListingID, existing);
            }

            if (
              expiredWithdrawnListingIds.has(row.L_ListingID) &&
              ["1", "2"].includes(row.L_StatusCatID)
            ) {
              console.log(
                `- ${row.L_ListingID} (${row.L_Address}): Status ${row.L_StatusCatID} -> REMOVED`
              );
              const existing = expiredMatches.get(row.L_ListingID) || {
                tables: [] as string[],
                address: row.L_Address,
                status: row.L_StatusCatID,
              };
              existing.tables.push(table);
              expiredMatches.set(row.L_ListingID, existing);
            }
          }
        }
      }
    }

    if (!foundAnyRecords) {
      console.log("\nNo matching records found in any table.");
    }

    // Group matches by table for efficient updates
    const soldByTable = groupByTable(soldMatches);
    const expiredByTable = groupByTable(expiredMatches);

    // Process SOLD listings
    if (soldMatches.size > 0) {
      console.log("\nProcessing SOLD listings...");
      for (const [table, listingIds] of soldByTable) {
        const updateSql = `UPDATE ${table} SET L_StatusCatID = '2' WHERE L_ListingID IN (?)`;
        await connection.query(updateSql, [listingIds]);
        console.log(
          `Updated ${listingIds.length} listings to SOLD status in ${table}:`
        );
        listingIds.forEach((id) => {
          const info = soldMatches.get(id);
          console.log(
            `- ${id} (${info?.address}): Status ${info?.status} -> SOLD`
          );
        });
      }
    }

    // Process EXPIRED/WITHDRAWN listings
    if (expiredMatches.size > 0) {
      console.log("\nProcessing EXPIRED/WITHDRAWN listings...");
      for (const [table, listingIds] of expiredByTable) {
        const deleteSql = `DELETE FROM ${table} WHERE L_ListingID IN (?)`;
        await connection.query(deleteSql, [listingIds]);
        console.log(
          `Deleted ${listingIds.length} EXPIRED/WITHDRAWN listings from ${table}:`
        );
        listingIds.forEach((id) => {
          const info = expiredMatches.get(id);
          console.log(
            `- ${id} (${info?.address}): Status ${info?.status} -> REMOVED`
          );
        });
      }
    }

    console.log("\nProperty record cleanup complete!");
  } catch (error) {
    console.error("Error cleaning property records:", error);
  } finally {
    connection.release();
  }
}

// Run the script
checkPropertyStatus().catch(console.error);

// Export the function for use in service.ts
export { checkPropertyStatus };
