import { RetsSession, createAuthenticatedOptions } from "../auth";
import { makeRetsRequest } from "../utils/http";
import * as fs from "fs/promises";
import * as path from "path";

export type PhotoType = "Agent" | "Office" | "Property";

// Simple multipart parser for RETS GetObject response
export function parseMultipartResponse(
  buffer: Buffer,
  boundary: string
): Buffer[] {
  const parts: Buffer[] = [];
  const boundaryBytes = Buffer.from(`\r\n--${boundary}`);

  let start = 0;
  while (true) {
    // Find the start of the next part
    const partStart = buffer.indexOf(boundaryBytes, start);
    if (partStart === -1) break;

    // Find the end of this part
    const partEnd = buffer.indexOf(
      boundaryBytes,
      partStart + boundaryBytes.length
    );
    if (partEnd === -1) break;

    // Extract the part content
    const partContent = buffer.subarray(
      partStart + boundaryBytes.length,
      partEnd
    );

    // Find the actual image data (skip headers)
    const headerEnd = partContent.indexOf("\r\n\r\n");
    if (headerEnd !== -1) {
      // Skip the headers and any extra newlines
      const imageData = partContent.subarray(headerEnd + 4);
      // Find the start of the JPEG data (ffd8)
      const jpegStart = imageData.indexOf(Buffer.from([0xff, 0xd8]));
      if (jpegStart !== -1) {
        parts.push(imageData.subarray(jpegStart));
      }
    }

    start = partEnd;
  }

  return parts;
}

export interface PhotoResult {
  photos: Buffer[];
  lastModified: Date | null;
}

export async function getPhoto(
  session: RetsSession,
  type: PhotoType,
  id: string
): Promise<PhotoResult> {
  console.log(`Fetching ${type} photo for ID: ${id}`);

  const getObjectUrl = session.capabilityUrls.GetObject;
  if (!getObjectUrl) {
    throw new Error("GetObject URL not found in capability URLs");
  }

  // Create query parameters for photo
  const queryParams = new URLSearchParams({
    Resource: type,
    Type: "Photo",
    ID: `${id}:*`,
    Location: "0",
  });

  // Make authenticated request with binary response type
  const options = {
    ...createAuthenticatedOptions(session, getObjectUrl, queryParams),
    responseType: "arraybuffer" as const,
  };

  const { response, headers } = await makeRetsRequest(options);

  // Convert response to Buffer
  const responseBuffer = Buffer.from(response);

  // Check if we got a valid response
  if (responseBuffer.length < 100) {
    console.log(
      `No photos found for ${type} ${id} - response too small (${responseBuffer.length} bytes)`
    );
    return { photos: [], lastModified: null };
  }

  // Parse multipart response
  const boundary = headers["content-type"]?.split("boundary=")[1];
  if (!boundary) {
    throw new Error("No boundary found in content-type header");
  }

  // Get the Last-Modified timestamp from headers
  const lastModified = headers["last-modified"]
    ? new Date(headers["last-modified"])
    : null;

  return {
    photos: parseMultipartResponse(responseBuffer, boundary),
    lastModified,
  };
}

export async function savePhotos(
  type: PhotoType,
  id: string,
  photos: Buffer[],
  lastModified: Date | null = null
): Promise<string[]> {
  // Create base photos directory in cache
  const baseDir = path.join(process.cwd(), "cache", "Photos");
  await fs.mkdir(baseDir, { recursive: true });

  const savedPaths: string[] = [];

  // Create type-specific directory
  const typeDir = path.join(baseDir, type);
  await fs.mkdir(typeDir, { recursive: true });

  // Create ID-specific directory for all types
  const idDir = path.join(typeDir, id);
  await fs.mkdir(idDir, { recursive: true });

  // Save each photo with consistent numbering
  for (let i = 0; i < photos.length; i++) {
    const photoPath = path.join(idDir, `${i}.jpg`);
    await fs.writeFile(photoPath, photos[i]);

    // Set the file's last modified time if we have it
    if (lastModified) {
      await fs.utimes(photoPath, lastModified, lastModified);
    }

    console.log(`Photo saved to: ${photoPath}`);
    savedPaths.push(photoPath);
  }

  return savedPaths;
}
