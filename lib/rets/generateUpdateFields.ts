import { RetsParser } from "./retsParser";
import {
  login,
  logout,
  createAuthenticatedOptions,
  RetsSession,
} from "../auth";
import * as fs from "fs/promises";
import * as path from "path";
import { makeRetsRequest } from "../utils/http";
import { UPDATE_FIELDS_CACHE_FILE } from "./updateFields";

// Function to fetch METADATA-RESOURCE
async function getResources(session: RetsSession): Promise<any[]> {
  const metadataUrl = session.capabilityUrls.GetMetadata;
  if (!metadataUrl) {
    throw new Error("GetMetadata URL not found in capability URLs");
  }

  const queryParams = new URLSearchParams({
    Type: "METADATA-RESOURCE",
    Format: "COMPACT",
    ID: "0",
  });

  const options = createAuthenticatedOptions(session, metadataUrl, queryParams);

  try {
    const { response } = await makeRetsRequest(options);
    const parsed = await RetsParser.parse(response);

    if (parsed.ReplyCode !== "0") {
      throw new Error(`METADATA-RESOURCE failed: ${parsed.ReplyText}`);
    }

    return parsed.Metadata?.Data || [];
  } catch (error) {
    throw new Error(
      `METADATA-RESOURCE error: ${
        error instanceof Error ? error.message : String(error)
      }`
    );
  }
}

// Function to fetch METADATA-TABLE for a specific resource and class
async function getTableMetadata(
  session: RetsSession,
  resourceId: string,
  classId: string
): Promise<any> {
  const metadataUrl = session.capabilityUrls.GetMetadata;
  if (!metadataUrl) {
    throw new Error("GetMetadata URL not found in capability URLs");
  }

  const queryParams = new URLSearchParams({
    Type: "METADATA-TABLE",
    Format: "COMPACT",
    ID: `${resourceId}:${classId}`,
  });

  const options = createAuthenticatedOptions(session, metadataUrl, queryParams);

  try {
    const { response } = await makeRetsRequest(options);
    const parsed = await RetsParser.parse(response);

    if (parsed.ReplyCode !== "0") {
      throw new Error(
        `METADATA-TABLE failed for ${resourceId}:${classId}: ${parsed.ReplyText}`
      );
    }

    return parsed.Metadata?.Data || [];
  } catch (error) {
    throw new Error(
      `METADATA-TABLE error for ${resourceId}:${classId}: ${
        error instanceof Error ? error.message : String(error)
      }`
    );
  }
}

// Function to fetch METADATA-CLASS for a specific resource
async function getClasses(
  session: RetsSession,
  resourceId: string
): Promise<any[]> {
  const metadataUrl = session.capabilityUrls.GetMetadata;
  if (!metadataUrl) {
    throw new Error("GetMetadata URL not found in capability URLs");
  }

  const queryParams = new URLSearchParams({
    Type: "METADATA-CLASS",
    Format: "COMPACT",
    ID: `${resourceId}:0`,
  });

  const options = createAuthenticatedOptions(session, metadataUrl, queryParams);

  try {
    const { response } = await makeRetsRequest(options);
    const parsed = await RetsParser.parse(response);

    if (parsed.ReplyCode !== "0") {
      throw new Error(
        `METADATA-CLASS failed for ${resourceId}: ${parsed.ReplyText}`
      );
    }

    return parsed.Metadata?.Data || [];
  } catch (error) {
    throw new Error(
      `METADATA-CLASS error for ${resourceId}: ${
        error instanceof Error ? error.message : String(error)
      }`
    );
  }
}

interface ResourceGroup {
  resource: string;
  updateField: string;
  syncInterval: number;
  syncType: "full" | "partial";
  classes: (string | null)[];
}

export async function generateUpdateFields(): Promise<void> {
  let session: RetsSession | null = null;
  const resources: ResourceGroup[] = [];
  try {
    // Perform Login
    session = await login();
    // Fetch all resources
    const allResources = await getResources(session);
    // Iterate through each resource
    for (const resource of allResources) {
      const classes = await getClasses(session, resource.ResourceID);
      let syncInterval = 1440;
      if (resource.ResourceID.includes("Property_")) {
        syncInterval = 1;
      } else if (
        ["Office", "ActiveOffice", "Agent", "ActiveAgent"].includes(
          resource.ResourceID
        )
      ) {
        syncInterval = 60;
      }
      let updateDateField: any = null;
      let classNames: (string | null)[] = [];
      if (classes.length === 0) {
        const tableMetadata = await getTableMetadata(
          session,
          resource.ResourceID,
          "0"
        );
        updateDateField = tableMetadata.find(
          (field: any) =>
            field.SystemName.match(/[A-Z]_UpdateDate$/) &&
            !field.SystemName.startsWith("U_") &&
            !field.SystemName.startsWith("O_")
        );
        classNames = [null];
      } else {
        // Use the first class to determine the shared params
        const firstClass = classes[0];
        const tableMetadata = await getTableMetadata(
          session,
          resource.ResourceID,
          firstClass.ClassName
        );
        updateDateField = tableMetadata.find(
          (field: any) =>
            field.SystemName.match(/[A-Z]_UpdateDate$/) &&
            !field.SystemName.startsWith("U_") &&
            !field.SystemName.startsWith("O_")
        );
        classNames = classes.map((cls: any) => cls.ClassName);
      }
      resources.push({
        resource: resource.ResourceID,
        updateField: updateDateField ? updateDateField.SystemName : "N/A",
        syncInterval: updateDateField ? syncInterval : 1440,
        syncType: updateDateField ? "partial" : "full",
        classes: classNames,
      });
    }
    // Save the results to a JSON file
    const outputPath = path.join(process.cwd(), "cache", "update_fields.json");
    await fs.mkdir(path.dirname(outputPath), { recursive: true });
    await fs.writeFile(outputPath, JSON.stringify(resources, null, 2), "utf8");
    console.log(`Update fields report generated at: ${outputPath}`);

    // Write the update fields to a file
    await fs.writeFile(
      UPDATE_FIELDS_CACHE_FILE,
      JSON.stringify(resources, null, 2)
    );
  } catch (error) {
    console.error(
      "Error:",
      error instanceof Error ? error.message : String(error)
    );
    throw error; // Re-throw the error to be handled by the caller
  } finally {
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

// Only run the script if this file is executed directly
if (require.main === module) {
  generateUpdateFields().catch((error) => {
    console.error("Failed to generate update fields:", error);
    process.exit(1);
  });
}
