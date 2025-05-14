import { login } from "./lib/auth";
import { getPhoto, savePhotos, PhotoType } from "./lib/rets/photos";
import {
  getPropertyPhotos,
  processPropertyPhotos,
} from "./lib/rets/propertyPhotos";

async function main(type: PhotoType, id: string): Promise<void> {
  try {
    console.log(`Starting photo processing for ${type} ${id}...`);

    // Login to RETS server
    console.log("Logging in to RETS server...");
    const session = await login();
    console.log("Successfully logged in");

    if (type === "Property") {
      // Get photos from RETS
      console.log("Fetching property photos...");
      const photos = await getPropertyPhotos(session, id);

      if (photos.length === 0) {
        console.log("No photos found for this property");
        return;
      }

      // Process and save photos
      console.log("Processing and saving photos...");
      const processedPhotos = await processPropertyPhotos(
        id,
        "RE_1" // Default to Residential property type
      );

      console.log(
        `Successfully processed ${processedPhotos.length} photos for listing ${id}`
      );

      // Extract just the /Photos/... part of the path
      const fullPath = processedPhotos[0].images.original.url;
      const photosIndex = fullPath.indexOf("/Photos/");
      if (photosIndex !== -1) {
        console.log(`Photos saved to: ${fullPath.substring(photosIndex)}`);
      }
    } else {
      // Get photos and timestamp for Agent or Office
      console.log(`Fetching ${type} photos...`);
      const { photos, lastModified } = await getPhoto(session, type, id);

      if (photos.length === 0) {
        console.log(`No photos found for this ${type}`);
        return;
      }

      // Save photos to disk with timestamp
      console.log("Saving photos...");
      await savePhotos(type, id, photos, lastModified);

      console.log(
        `Successfully saved ${photos.length} photos for ${type} ${id}`
      );
    }
  } catch (error) {
    console.error(`Error fetching photo for ${type} ${id}:`, error);
    process.exit(1);
  }
}

// Parse command line arguments
const args = process.argv.slice(2);
if (args.length !== 2) {
  console.log("Usage: bun getPhotos.ts <type> <id>");
  console.log("Example: bun getPhotos.ts Agent 42");
  console.log("Example: bun getPhotos.ts Office 5");
  console.log("Example: bun getPhotos.ts Property 230475");
  process.exit(1);
}

const type = args[0] as PhotoType;
const id = args[1];

if (type !== "Agent" && type !== "Office" && type !== "Property") {
  console.error("Type must be either 'Agent', 'Office', or 'Property'");
  process.exit(1);
}

// Run the script
main(type, id).catch(console.error);
