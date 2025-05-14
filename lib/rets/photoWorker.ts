import { getPropertyPhotos, processPropertyPhotos } from "./propertyPhotos";
import { parentPort } from "worker_threads";
import { RetsSession } from "../auth/auth";
import { getConnection } from "../db/db";
import { RowDataPacket } from "mysql2";

// Get the listing ID and property type from the worker data
const listingId = process.env.listingId as string;
const propertyType = process.env.propertyType as string;
const sessionId = process.env.sessionId as string;
const capabilityUrls = JSON.parse(process.env.capabilityUrls as string);

if (!listingId || !propertyType || !sessionId || !capabilityUrls) {
  console.error("Missing required worker data");
  process.exit(1);
}

// Create session from passed data
const session: RetsSession = {
  sessionId,
  capabilityUrls,
};

// Process the listing's photos
async function processPhotos() {
  try {
    console.log(`\n=== Starting Photo Processing for Listing ${listingId} ===`);
    console.log(`Property Type: ${propertyType}`);

    // Get L_Last_Photo_updt and L_PictureCount from database
    console.log(`Fetching listing details from database...`);
    const conn = await getConnection();
    const [rows] = await conn.query<RowDataPacket[]>(
      `SELECT L_Last_Photo_updt, L_PictureCount FROM Property_${propertyType} WHERE L_ListingID = ?`,
      [listingId]
    );
    const lastPhotoUpdate = rows[0]?.L_Last_Photo_updt || null;
    const pictureCount = rows[0]?.L_PictureCount || 0;
    console.log(`Found listing with ${pictureCount} expected photos`);
    console.log(
      `Last photo update: ${lastPhotoUpdate?.toISOString() || "N/A"}`
    );

    console.log(`\nFetching photos from RETS server...`);
    const photos = await getPropertyPhotos(session, listingId);
    console.log(`Fetched ${photos.length} photos from RETS server`);

    if (photos.length > 0) {
      // Validate photos before processing
      console.log(`\nValidating photos...`);
      const validPhotos = photos.filter((photo) => {
        if (!photo.imageData || photo.imageData.length === 0) {
          console.log(`✗ Skipping invalid photo (empty data)`);
          return false;
        }
        console.log(
          `✓ Valid photo found (${(
            photo.imageData.length /
            1024 /
            1024
          ).toFixed(2)} MB)`
        );
        return true;
      });

      console.log(`\nFound ${validPhotos.length} valid photos to process`);

      if (validPhotos.length > 0) {
        console.log(`\nStarting photo processing...`);
        try {
          await processPropertyPhotos(listingId, propertyType);
          console.log(
            `\n✓ Successfully processed all photos for listing ${listingId}`
          );
          if (parentPort) {
            parentPort.postMessage("done");
          } else {
            console.error("No parent port available to send message");
          }
        } catch (error) {
          console.error(`\n✗ Error processing photos for ${listingId}:`, error);
          if (parentPort) {
            parentPort.postMessage("error");
          } else {
            console.error("No parent port available to send message");
          }
        }
      } else {
        console.log(`\nNo valid photos found for ${listingId}`);
        if (parentPort) {
          parentPort.postMessage("done");
        } else {
          console.error("No parent port available to send message");
        }
      }
    } else {
      console.log(`\nNo photos found for ${listingId}`);
      if (parentPort) {
        parentPort.postMessage("done");
      } else {
        console.error("No parent port available to send message");
      }
    }
  } catch (error) {
    console.error(`\n✗ Fatal error processing photos for ${listingId}:`, error);
    if (parentPort) {
      parentPort.postMessage("error");
    } else {
      console.error("No parent port available to send message");
    }
  }
}

// Start processing
processPhotos().catch((error) => {
  console.error("Unhandled error in photo worker:", error);
  process.exit(1);
});
