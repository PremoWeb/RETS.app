import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import * as fs from "fs/promises";
import * as path from "path";
import * as mime from "mime-types";
import { createReadStream } from "fs";
import { stat } from "fs/promises";

// Vultr Object Storage configuration
if (
  !process.env.OBJECT_STORAGE_ACCESS_KEY ||
  !process.env.OBJECT_STORAGE_SECRET_KEY ||
  !process.env.OBJECT_STORAGE_ENDPOINT ||
  !process.env.OBJECT_STORAGE_BUCKET
) {
  throw new Error("Missing required object storage environment variables");
}

const config = {
  endpoint: `https://${process.env.OBJECT_STORAGE_ENDPOINT}`,
  region: process.env.OBJECT_STORAGE_ENDPOINT.split(".")[0] || "sjc1",
  credentials: {
    accessKeyId: process.env.OBJECT_STORAGE_ACCESS_KEY as string,
    secretAccessKey: process.env.OBJECT_STORAGE_SECRET_KEY as string,
  },
  forcePathStyle: true,
  requestHandler: {
    httpOptions: {
      timeout: 300000, // 5 minutes
      keepAlive: true,
      maxSockets: 50,
      headers: {
        "User-Agent": "RETS.app-Photo-Sync/1.0",
        Accept: "*/*",
        Connection: "keep-alive",
      },
    },
  },
};

// Initialize S3 client with retry configuration
const s3Client = new S3Client({
  ...config,
  maxAttempts: 5,
});

// Track successful uploads by listing ID
const successfulUploads = new Map<string, Set<string>>();

async function uploadFile(
  filePath: string,
  key: string,
  retries = 5
): Promise<void> {
  const fileStats = await stat(filePath);
  const contentType = mime.lookup(filePath) || "application/octet-stream";

  // For files smaller than 5MB, use buffer instead of stream
  const useBuffer = fileStats.size < 5 * 1024 * 1024;

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      let body;
      if (useBuffer) {
        // Read file into buffer
        body = await fs.readFile(filePath);
      } else {
        // Use stream for larger files
        const fileStream = createReadStream(filePath);
        await new Promise<void>((resolve) => {
          fileStream.once("readable", () => resolve());
        });
        body = fileStream;
      }

      await s3Client.send(
        new PutObjectCommand({
          Bucket: process.env.OBJECT_STORAGE_BUCKET,
          Key: key,
          Body: body,
          ContentType: contentType,
          ContentLength: fileStats.size,
          ACL: "public-read",
        })
      );

      if (!useBuffer) {
        (body as any).destroy?.();
      }

      console.log(`Uploaded (${fileStats.size} bytes): ${key}`);

      // Track successful upload
      const listingId = path.basename(path.dirname(filePath));
      const size = path.basename(filePath, path.extname(filePath));
      if (!successfulUploads.has(listingId)) {
        successfulUploads.set(listingId, new Set());
      }
      successfulUploads.get(listingId)?.add(size);

      // Check if this listing is now complete and can be cleaned up
      await checkAndCleanupListing(listingId, filePath);

      return;
    } catch (error) {
      if (attempt === retries) {
        console.error(
          `Failed to upload ${key} after ${retries} attempts:`,
          error
        );
        throw error;
      }

      // Exponential backoff with jitter
      const baseDelay = 1000; // 1 second
      const maxDelay = 30000; // 30 seconds
      const jitter = Math.random() * 0.1; // 10% jitter
      const delay = Math.min(
        baseDelay * Math.pow(2, attempt - 1) * (1 + jitter),
        maxDelay
      );

      console.log(
        `Retry ${attempt}/${retries} for ${key} (waiting ${Math.round(
          delay / 1000
        )}s)`
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

async function* walkDir(dir: string): AsyncGenerator<string> {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      yield* walkDir(fullPath);
    } else {
      yield fullPath;
    }
  }
}

// Add new function to check and cleanup a listing
async function checkAndCleanupListing(
  listingId: string,
  filePath: string
): Promise<void> {
  const sizes = successfulUploads.get(listingId);
  if (!sizes) return;

  const requiredSizes = new Set([
    "original",
    "large",
    "medium",
    "small",
    "thumb",
  ]);
  const hasAllSizes = [...requiredSizes].every((size) =>
    [...sizes].some((s) => s.startsWith(size))
  );

  if (hasAllSizes) {
    const photosBaseDir = path.join(process.cwd(), "cache", "Photos");
    // Get the property type from the path (e.g., Residential, Land, Commercial, Multi-Family)
    const propertyType = path.basename(path.dirname(path.dirname(filePath)));
    const listingDir = path.join(photosBaseDir, propertyType, listingId);

    try {
      console.log(
        `\nChecking listing ${listingId} in ${propertyType} for cleanup...`
      );
      console.log(`Found sizes: ${[...sizes].join(", ")}`);

      // Verify directory exists before trying to remove
      try {
        await fs.access(listingDir);
        console.log(`Directory exists, proceeding with cleanup...`);
      } catch (error) {
        console.log(`Directory already removed or not found: ${listingDir}`);
        return;
      }

      // Remove directory
      await fs.rm(listingDir, { recursive: true, force: true });
      console.log(
        `Successfully cleaned up local files for listing ${listingId} in ${propertyType}`
      );
    } catch (error) {
      console.error(
        `Failed to clean up listing ${listingId} in ${propertyType}:`,
        error
      );
    }
  }
}

async function syncPhotosToObjectStorage(): Promise<void> {
  const photosBaseDir = path.join(process.cwd(), "cache", "Photos");

  try {
    // Check if Photos directory exists
    await fs.access(photosBaseDir);

    console.log("Starting photo sync to object storage...");
    let successCount = 0;
    let errorCount = 0;
    let totalFiles = 0;
    let cleanedCount = 0;

    // Count total files first
    for await (const _ of walkDir(photosBaseDir)) {
      totalFiles++;
    }
    console.log(`Found ${totalFiles} files to process\n`);

    // Walk through all files in the Photos directory
    for await (const filePath of walkDir(photosBaseDir)) {
      try {
        // Get relative path for S3 key, preserving the property type directory
        const relativePath = path.relative(photosBaseDir, filePath);
        const s3Key = `Photos/${relativePath}`;

        const progress = (
          ((successCount + errorCount + 1) / totalFiles) *
          100
        ).toFixed(1);
        process.stdout.write(
          `\rProgress: ${progress}% (${
            successCount + errorCount + 1
          }/${totalFiles}) - Success: ${successCount} - Cleaned: ${cleanedCount}`
        );

        await uploadFile(filePath, s3Key);
        successCount++;
      } catch (error) {
        // Check if this is an expected "file not found" error due to cleanup
        if (error.code === "ENOENT") {
          // Extract listing ID and property type from the path
          const pathParts = filePath.split(path.sep);
          const listingId = pathParts[pathParts.length - 2];
          const propertyType = pathParts[pathParts.length - 3];

          // Only log if we haven't already cleaned up this listing
          if (!successfulUploads.has(listingId)) {
            console.log(
              `\nSkipping ${filePath} - directory already cleaned up`
            );
          }
        } else {
          console.error(`\nFailed to process ${filePath}:`, error);
        }
        errorCount++;
      }
    }

    console.log("\n\nSync completed!");
    console.log(`Successfully uploaded: ${successCount} files`);
    console.log(`Skipped (already cleaned): ${errorCount} files`);
    console.log(`Cleaned up ${cleanedCount} completed listings`);
  } catch (error) {
    if (error.code === "ENOENT") {
      console.error("Photos directory not found!");
    } else {
      console.error("Error during sync:", error);
    }
    process.exit(1);
  }
}

// Run the sync
syncPhotosToObjectStorage().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
