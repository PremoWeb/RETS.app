import { getConnection } from "../db/db";
import * as fs from "fs/promises";
import * as path from "path";
import sharp from "sharp";
import { createAuthenticatedOptions } from "../auth";
import { makeRetsRequest } from "../utils/http";
import * as https from "https";
import { RowDataPacket } from "mysql2";
import { RetsParser } from "./retsParser";
import { login, RetsSession } from "../auth/auth";
import { fileTypeFromBuffer } from "file-type";
import { S3Client } from "bun";

// Initialize S3 client for Vultr Object Storage
const s3 = new S3Client({
  accessKeyId: process.env.OBJECT_STORAGE_ACCESS_KEY,
  secretAccessKey: process.env.OBJECT_STORAGE_SECRET_KEY,
  bucket: process.env.OBJECT_STORAGE_BUCKET,
  endpoint: `https://${process.env.OBJECT_STORAGE_ENDPOINT}`,
  region: process.env.OBJECT_STORAGE_ENDPOINT?.split(".")[0] || "sjc1",
  acl: "public-read",
});

// Ensure bucket exists
async function ensureBucketExists() {
  try {
    // Try to read a test file to check if bucket exists
    const testFile = s3.file(".test");
    await testFile.exists();
    console.log(`Bucket ${process.env.OBJECT_STORAGE_BUCKET} exists`);
  } catch (error) {
    console.log(`Creating bucket ${process.env.OBJECT_STORAGE_BUCKET}...`);
    // Create a test file to ensure bucket exists
    await s3.write(".test", "test");
    console.log(`Bucket ${process.env.OBJECT_STORAGE_BUCKET} created`);
  }
}

export interface PropertyPhoto {
  data: Buffer;
  metadata: {
    lastModified?: string;
    contentSubDescription?: string;
    contentLabel?: string;
    accessibility?: string;
    photoTimestamp?: string;
  };
}

export interface PropertyClass {
  class: "Residential" | "Land" | "Commercial" | "Multi-Family";
  table: string;
}

interface PhotoResponse {
  objectId: string;
  imageData: Buffer;
  metadata: {
    lastModified?: string;
    contentSubDescription?: string;
    contentLabel?: string;
    accessibility?: string;
    contentType?: string;
    objectId?: string;
    preferred?: string;
    description?: string;
    location?: string;
    type?: string;
    size?: string;
    width?: string;
    height?: string;
    [key: string]: string | undefined; // Allow any X- headers
  };
}

export interface ProcessedPhoto {
  objectId: string;
  dominantColor: string;
  images: {
    original: {
      url: string;
      width: number;
      height: number;
      fileSize: number;
      format: string;
    };
    large: {
      url: string;
      width: number;
      height: number;
      fileSize: number;
      format: string;
    };
    medium: {
      url: string;
      width: number;
      height: number;
      fileSize: number;
      format: string;
    };
    small: {
      url: string;
      width: number;
      height: number;
      fileSize: number;
      format: string;
    };
    thumb: {
      url: string;
      width: number;
      height: number;
      fileSize: number;
      format: string;
    };
  };
  metadata: {
    contentSubDescription?: string;
    "x-content-label"?: string;
    "x-accessibility"?: string;
    lastModified?: string;
  };
}

// Function to get photos from RETS
export async function getPropertyPhotos(
  session: RetsSession,
  listingId: string
): Promise<PhotoResponse[]> {
  try {
    // Get the GetObject URL from capabilities
    const getObjectUrl = session.capabilityUrls.GetObject;
    if (!getObjectUrl) {
      throw new Error("GetObject URL not found in capabilities");
    }

    // Create authenticated request options
    const options = createAuthenticatedOptions(
      session,
      getObjectUrl,
      new URLSearchParams({
        Resource: "Property",
        Type: "Photo",
        ID: `${listingId}:*`,
      })
    ) as https.RequestOptions & { responseType?: "arraybuffer" | "text" };
    options.responseType = "arraybuffer";

    // Make the RETS request
    const { response, headers } = await makeRetsRequest(options);
    const responseBuffer = Buffer.isBuffer(response)
      ? response
      : Buffer.from(response);

    // Parse multipart response
    const photos: PhotoResponse[] = [];
    const contentType = headers?.["content-type"] as string;
    const boundary = contentType?.match(/boundary=([^;]+)/)?.[1];

    if (!boundary) {
      // If no boundary, treat as single image
      photos.push({
        objectId: listingId,
        imageData: responseBuffer,
        metadata: {},
      });
    } else {
      // Parse multipart response
      const boundaryBuffer = Buffer.from(`\r\n--${boundary}`);
      const endBoundaryBuffer = Buffer.from(`\r\n--${boundary}--`);
      let currentPos = 0;

      // Skip first boundary
      const firstBoundaryPos = responseBuffer.indexOf(boundaryBuffer.slice(2));
      if (firstBoundaryPos !== -1) {
        currentPos = firstBoundaryPos + boundaryBuffer.length - 2;
      }

      while (currentPos < responseBuffer.length) {
        // Skip \r\n after boundary if present
        if (
          responseBuffer[currentPos] === 13 &&
          responseBuffer[currentPos + 1] === 10
        ) {
          currentPos += 2;
        }

        // Find end of headers (double \r\n)
        const headersEndPos = responseBuffer.indexOf(
          Buffer.from("\r\n\r\n"),
          currentPos
        );
        if (headersEndPos === -1) break;

        // Parse headers
        const headersStr = responseBuffer
          .slice(currentPos, headersEndPos)
          .toString();
        const headerLines = headersStr.split("\r\n");
        const headerMap: { [key: string]: string } = {};

        for (const line of headerLines) {
          const [key, value] = line.split(": ");
          if (key && value) {
            headerMap[key.toLowerCase()] = value;
          }
        }

        // Move past headers
        currentPos = headersEndPos + 4;

        // Find next boundary
        const nextBoundaryPos = responseBuffer.indexOf(
          boundaryBuffer,
          currentPos
        );
        const endBoundaryPos = responseBuffer.indexOf(
          endBoundaryBuffer,
          currentPos
        );

        // Determine where this part ends
        let partEndPos: number;
        if (
          endBoundaryPos !== -1 &&
          (nextBoundaryPos === -1 || endBoundaryPos < nextBoundaryPos)
        ) {
          // This is the last part
          partEndPos = endBoundaryPos;
        } else if (nextBoundaryPos !== -1) {
          // There's another part after this
          partEndPos = nextBoundaryPos;
        } else {
          // No more boundaries found
          break;
        }

        // Extract binary data
        const binaryData = responseBuffer.slice(currentPos, partEndPos);

        // Extract object ID from headers
        const objectId = headerMap["object-id"] || listingId;
        const contentType = headerMap["content-type"];

        if (!contentType || !contentType.startsWith("image/")) {
          continue;
        }

        if (binaryData.length > 0) {
          // Extract all X- headers
          const xHeaders: { [key: string]: string } = {};
          for (const [key, value] of Object.entries(headerMap)) {
            if (key.startsWith("x-")) {
              xHeaders[key] = value;
            }
          }

          photos.push({
            objectId,
            imageData: binaryData,
            metadata: {
              lastModified: headerMap["last-modified"],
              contentSubDescription: headerMap["content-sub-description"],
              contentLabel: headerMap["content-label"],
              accessibility: headerMap["accessibility"],
              photoTimestamp: headerMap["photo-timestamp"],
              preferred: headerMap["preferred"],
              description: headerMap["description"],
              location: headerMap["location"],
              type: headerMap["type"],
              size: headerMap["size"],
              width: headerMap["width"],
              height: headerMap["height"],
              ...xHeaders,
            },
          });
        }

        // Move to next part
        currentPos = partEndPos;

        // If we found the end boundary, we're done
        if (endBoundaryPos !== -1 && endBoundaryPos <= partEndPos) {
          break;
        }
      }
    }

    return photos;
  } catch (error) {
    console.error(`Error fetching photos for listing ${listingId}:`, error);
    throw error;
  }
}

interface PropertyClassRow extends RowDataPacket {
  class: "Residential" | "Land" | "Commercial" | "Multi-Family";
  table_name: string;
}

export async function getPropertyClass(
  listingId: string
): Promise<PropertyClass> {
  console.log(`Determining property class for listing ${listingId}...`);
  const connection = await getConnection();
  try {
    const query = `
      SELECT 
        CASE 
          WHEN EXISTS (SELECT 1 FROM Property_RE_1 WHERE L_ListingID = ?) THEN 'Residential'
          WHEN EXISTS (SELECT 1 FROM Property_LD_2 WHERE L_ListingID = ?) THEN 'Land'
          WHEN EXISTS (SELECT 1 FROM Property_CI_3 WHERE L_ListingID = ?) THEN 'Commercial'
          WHEN EXISTS (SELECT 1 FROM Property_MF_4 WHERE L_ListingID = ?) THEN 'Multi-Family'
        END as class,
        CASE 
          WHEN EXISTS (SELECT 1 FROM Property_RE_1 WHERE L_ListingID = ?) THEN 'Property_RE_1'
          WHEN EXISTS (SELECT 1 FROM Property_LD_2 WHERE L_ListingID = ?) THEN 'Property_LD_2'
          WHEN EXISTS (SELECT 1 FROM Property_CI_3 WHERE L_ListingID = ?) THEN 'Property_CI_3'
          WHEN EXISTS (SELECT 1 FROM Property_MF_4 WHERE L_ListingID = ?) THEN 'Property_MF_4'
        END as table_name
      FROM dual
    `;

    const [rows] = await connection.query<PropertyClassRow[]>(query, [
      listingId,
      listingId,
      listingId,
      listingId,
      listingId,
      listingId,
      listingId,
      listingId,
    ]);

    if (!rows || rows.length === 0) {
      throw new Error(`Property class not found for listing ID: ${listingId}`);
    }

    const result = {
      class: rows[0].class,
      table: rows[0].table_name,
    } as PropertyClass;

    console.log(
      `Property class determined: ${result.class} (table: ${result.table})`
    );
    return result;
  } finally {
    connection.release();
  }
}

// Function to get dominant color from image
async function getDominantColor(imageBuffer: Buffer): Promise<string> {
  const image = sharp(imageBuffer);
  const { dominant } = await image.stats();
  return `${dominant.r},${dominant.g},${dominant.b}`;
}

async function syncToCDN(
  listingId: string,
  propertyType: string,
  imagePath: string,
  size: string
): Promise<string> {
  const key = `photos/${propertyType}/${listingId}/${size}/${path.basename(
    imagePath
  )}`;

  try {
    const fileContent = await fs.readFile(imagePath);
    const fileType = await fileTypeFromBuffer(fileContent);
    const contentType = fileType?.mime || "image/webp";

    console.log(`Uploading ${key} to CDN...`);
    const file = s3.file(key);
    await s3.write(key, fileContent);
    console.log(`Successfully uploaded ${key} to CDN`);

    // Clean up local file after successful upload
    await fs.unlink(imagePath);
    console.log(`Cleaned up local file: ${imagePath}`);

    return `https://sjc1.vultrobjects.com/${process.env.OBJECT_STORAGE_BUCKET}/${key}`;
  } catch (error) {
    console.error(`Error syncing ${key} to CDN:`, error);
    throw error;
  }
}

// Function to sync all files in a directory to CDN
async function syncDirectoryToCDN(
  listingId: string,
  propertyType: string,
  directory: string
): Promise<void> {
  try {
    const files = await fs.readdir(directory);
    console.log(`Uploading ${files.length} files to CDN...`);

    // Map property type to correct class name for CDN path
    const propertyClass =
      propertyType === "RE_1"
        ? "Residential"
        : propertyType === "MF_4"
        ? "MultiFamily"
        : propertyType === "CI_3"
        ? "Commercial"
        : propertyType === "LD_2"
        ? "Land"
        : "Unknown";

    // Upload all files in parallel
    await Promise.all(
      files.map(async (file) => {
        const filePath = path.join(directory, file);
        const stats = await fs.stat(filePath);

        if (stats.isFile()) {
          const key = `Photos/${propertyClass}/${listingId}/${file}`;
          const fileContent = await fs.readFile(filePath);
          const fileType = await fileTypeFromBuffer(fileContent);
          const contentType = fileType?.mime || "application/octet-stream";

          await s3.write(key, fileContent);
        }
      })
    );

    console.log(`Successfully uploaded to CDN`);

    // After all files are synced, remove the entire directory
    await fs.rm(directory, { recursive: true, force: true });
  } catch (error) {
    console.error(`Error syncing to CDN:`, error);
    throw error;
  }
}

// Main function to process property photos
export async function processPropertyPhotos(
  listingId: string,
  propertyType: string
): Promise<ProcessedPhoto[]> {
  try {
    // Ensure bucket exists before processing
    await ensureBucketExists();

    console.log(
      `Processing photos for listing ${listingId} of type ${propertyType}`
    );

    // Get the session
    const session = await login();
    if (!session) {
      throw new Error("Failed to establish RETS session");
    }

    // Get photos from RETS server
    const photos = await getPropertyPhotos(session, listingId);
    if (!photos || photos.length === 0) {
      console.log(`No photos found for listing ${listingId}`);
      return [];
    }

    // Create directory for photos
    const propertyClass =
      propertyType === "RE_1"
        ? "Residential"
        : propertyType === "MF_4"
        ? "MultiFamily"
        : propertyType === "CI_3"
        ? "Commercial"
        : propertyType === "LD_2"
        ? "Land"
        : "Unknown";
    const photoDir = path.join(
      "cache",
      "Photos",
      propertyClass,
      String(listingId)
    );
    await fs.mkdir(photoDir, { recursive: true });

    // Process each photo
    const processedPhotos = await Promise.all(
      photos.map(async (photo, index) => {
        const objectId = photo.objectId;
        const imageData = photo.imageData;
        const metadata = photo.metadata;

        try {
          // Create sharp instance with optimized settings
          let image = sharp(imageData, {
            failOnError: false,
            limitInputPixels: false,
          });

          // First try to get metadata to determine format
          try {
            const imageMetadata = await image.metadata();
            // If no format detected or format is not JPEG, try to force JPEG
            if (!imageMetadata.format || imageMetadata.format !== "jpeg") {
              const jpegBuffer = await image.jpeg().toBuffer();
              image = sharp(jpegBuffer, {
                failOnError: false,
                limitInputPixels: false,
              });
            }

            // Get dimensions
            const { width = 0, height = 0 } = imageMetadata;

            // Calculate dominant color
            const dominantColor = await getDominantColor(imageData);

            // Process and save different sizes in parallel
            const [original, large, medium, small, thumb] = await Promise.all([
              // Original - just convert to WebP
              image
                .clone()
                .webp({ quality: 90, effort: 4 })
                .toFile(path.join(photoDir, `original-${objectId}.webp`))
                .then(async (info) => ({
                  url: path.join(
                    "Photos",
                    propertyClass,
                    String(listingId),
                    `original-${objectId}.webp`
                  ),
                  width: info.width,
                  height: info.height,
                  fileSize: info.size,
                  format: "webp",
                })),

              // Large - 1920px width
              image
                .clone()
                .resize(1920, null, { withoutEnlargement: true })
                .webp({ quality: 85, effort: 4 })
                .toFile(path.join(photoDir, `large-${objectId}.webp`))
                .then(async (info) => ({
                  url: path.join(
                    "Photos",
                    propertyClass,
                    String(listingId),
                    `large-${objectId}.webp`
                  ),
                  width: info.width,
                  height: info.height,
                  fileSize: info.size,
                  format: "webp",
                })),

              // Medium - 1280px width
              image
                .clone()
                .resize(1280, null, { withoutEnlargement: true })
                .webp({ quality: 80, effort: 4 })
                .toFile(path.join(photoDir, `medium-${objectId}.webp`))
                .then(async (info) => ({
                  url: path.join(
                    "Photos",
                    propertyClass,
                    String(listingId),
                    `medium-${objectId}.webp`
                  ),
                  width: info.width,
                  height: info.height,
                  fileSize: info.size,
                  format: "webp",
                })),

              // Small - 800px width
              image
                .clone()
                .resize(800, null, { withoutEnlargement: true })
                .webp({ quality: 75, effort: 4 })
                .toFile(path.join(photoDir, `small-${objectId}.webp`))
                .then(async (info) => ({
                  url: path.join(
                    "Photos",
                    propertyClass,
                    String(listingId),
                    `small-${objectId}.webp`
                  ),
                  width: info.width,
                  height: info.height,
                  fileSize: info.size,
                  format: "webp",
                })),

              // Thumbnail - 400px width
              image
                .clone()
                .resize(400, null, { withoutEnlargement: true })
                .webp({ quality: 70, effort: 4 })
                .toFile(path.join(photoDir, `thumb-${objectId}.webp`))
                .then(async (info) => ({
                  url: path.join(
                    "Photos",
                    propertyClass,
                    String(listingId),
                    `thumb-${objectId}.webp`
                  ),
                  width: info.width,
                  height: info.height,
                  fileSize: info.size,
                  format: "webp",
                })),
            ]);

            return {
              objectId,
              dominantColor,
              images: {
                original,
                large,
                medium,
                small,
                thumb,
              },
              metadata: {
                contentSubDescription: metadata.contentSubDescription,
                "x-content-label": metadata.contentLabel,
                "x-accessibility": metadata.accessibility,
                lastModified: metadata.lastModified,
              },
            };
          } catch (error) {
            console.error(`Error processing photo ${objectId}:`, error);
            throw error;
          }
        } catch (error) {
          console.error(`Error processing photo ${objectId}:`, error);
          return null;
        }
      })
    );

    // Filter out null results and save metadata
    const validPhotos = processedPhotos.filter(
      (photo): photo is NonNullable<typeof photo> => photo !== null
    );

    // Save metadata as JSON
    const metadataPath = path.join(photoDir, "metadata.json");
    await fs.writeFile(metadataPath, JSON.stringify(validPhotos, null, 2));
    console.log(
      `Saved metadata for ${validPhotos.length} photos to ${metadataPath}`
    );

    // Sync all files to CDN
    await syncDirectoryToCDN(listingId, propertyType, photoDir);

    return validPhotos;
  } catch (error) {
    console.error(`Error processing photos for listing ${listingId}:`, error);
    throw error;
  }
}
