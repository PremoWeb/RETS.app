import { RetsParser } from "../../rets/retsParser";
import { createAuthenticatedOptions, RetsSession } from "../../auth";
import { makeRetsRequest } from "../../utils/http";

// Function to fetch METADATA-RESOURCE
export async function getResources(session: RetsSession): Promise<any[]> {
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
export async function getTableMetadata(
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
export async function getClasses(
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
