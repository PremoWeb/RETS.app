import { login, createAuthenticatedOptions, RetsSession } from "./lib/auth";
import { makeRetsRequest } from "./lib/utils/http";
import * as fs from "fs/promises";
import * as path from "path";

// Function to sleep for a specified number of milliseconds
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type PhotoType = "Agent" | "Office";

async function getRetsPhoto(
  session: RetsSession,
  type: PhotoType,
  id: string
): Promise<boolean> {
  try {
    console.log(`Fetching ${type} photo for ID: ${id}`);

    const getObjectUrl = session.capabilityUrls.GetObject;
    if (!getObjectUrl) {
      throw new Error("GetObject URL not found in capability URLs");
    }

    // Create query parameters for photo
    const queryParams = new URLSearchParams({
      Resource: type,
      Type: "Photo",
      ID: `${id}:0`,
      Location: "0",
    });

    // Make authenticated request with binary response type
    const options = {
      ...createAuthenticatedOptions(session, getObjectUrl, queryParams),
      responseType: "arraybuffer" as const,
    };

    const { response, headers } = await makeRetsRequest(options);

    // Convert response to Buffer
    const photoBuffer = Buffer.from(response);

    // Check if we got a valid image
    if (photoBuffer.length < 100) {
      // Arbitrary minimum size
      console.log(
        `Skipping ${type} ${id} - response too small (${photoBuffer.length} bytes)`
      );
      return false;
    }

    // Create cache directory if it doesn't exist
    const cacheDir = path.join(
      process.cwd(),
      "cache",
      `${type.toLowerCase()}_photos`
    );
    await fs.mkdir(cacheDir, { recursive: true });

    // Save the photo
    const photoPath = path.join(cacheDir, `${type.toLowerCase()}_${id}.jpg`);
    await fs.writeFile(photoPath, photoBuffer);

    console.log(`Photo saved to: ${photoPath}`);
    return true;
  } catch (error) {
    console.error(`Error fetching photo for ${type} ${id}:`, error);
    return false;
  }
}

async function fetchAllPhotos(type: PhotoType, startId: number, endId: number) {
  try {
    // Login once at the start
    const session = await login();
    console.log("Logged in successfully");

    let successCount = 0;
    let failCount = 0;

    for (let id = startId; id <= endId; id++) {
      const success = await getRetsPhoto(session, type, id.toString());
      if (success) {
        successCount++;
      } else {
        failCount++;
      }

      // Add a small delay between requests to be nice to the server
      if (id < endId) {
        await sleep(100); // 100ms delay between requests
      }
    }

    console.log(`\n${type} photo fetch complete!`);
    console.log(`Successfully fetched: ${successCount} photos`);
    console.log(`Failed to fetch: ${failCount} photos`);
    console.log(`Total attempted: ${endId - startId + 1} ${type}s`);
  } catch (error) {
    console.error("Fatal error:", error);
    process.exit(1);
  }
}

// Parse command line arguments
const args = process.argv.slice(2);
if (args.length < 3) {
  console.log("Usage: bun fetchRetsPhotos.ts <type> <startId> <endId>");
  console.log("Example: bun fetchRetsPhotos.ts Agent 1 300");
  console.log("Example: bun fetchRetsPhotos.ts Office 1 50");
  process.exit(1);
}

const type = args[0] as PhotoType;
const startId = parseInt(args[1]);
const endId = parseInt(args[2]);

if (type !== "Agent" && type !== "Office") {
  console.error("Type must be either 'Agent' or 'Office'");
  process.exit(1);
}

if (isNaN(startId) || isNaN(endId) || startId > endId) {
  console.error("Invalid ID range");
  process.exit(1);
}

// Run the script
fetchAllPhotos(type, startId, endId).catch(console.error);
