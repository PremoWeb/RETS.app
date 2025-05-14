import { serve } from "bun";
import * as path from "path";
import * as fs from "fs/promises";
import { fileTypeFromBuffer } from "file-type";

// Configuration
const PORT = process.env.PHOTO_PORT || 3000;
const CACHE_DIR = path.join(process.cwd(), "cache");
const MAX_AGE = 60 * 60 * 24 * 7; // 1 week in seconds

// Helper function to get file extension from content type
function getExtension(contentType: string): string {
  const map: Record<string, string> = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/gif": "gif",
  };
  return map[contentType] || "bin";
}

// Helper function to get content type from file extension
function getContentType(ext: string): string {
  const map: Record<string, string> = {
    jpg: "image/jpeg",
    jpeg: "image/jpeg",
    png: "image/png",
    webp: "image/webp",
    gif: "image/gif",
  };
  return map[ext] || "application/octet-stream";
}

// Helper function to check if file exists
async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

// Helper function to get last modified time
async function getLastModified(filePath: string): Promise<Date> {
  const stats = await fs.stat(filePath);
  return stats.mtime;
}

export function startPhotoServer() {
  const server = serve({
    port: PORT,
    async fetch(req) {
      const url = new URL(req.url);
      const pathname = url.pathname;

      // Handle health check
      if (pathname === "/health") {
        return new Response("OK", { status: 200 });
      }

      // Only handle /Photos/ paths
      if (!pathname.startsWith("/Photos/")) {
        return new Response("Not Found", { status: 404 });
      }

      try {
        // Parse the path components
        const parts = pathname.split("/").filter(Boolean);
        if (parts.length < 3) {
          return new Response("Invalid path", { status: 400 });
        }

        // Handle simplified agent photo path (e.g., /Photos/Agent/42.jpg)
        if (
          parts[1] === "Agent" &&
          parts.length === 3 &&
          parts[2].endsWith(".jpg")
        ) {
          const agentId = parts[2].replace(".jpg", "");
          const actualPath = `/Photos/Agent/${agentId}/0.jpg`;
          const actualFilePath = path.join(CACHE_DIR, actualPath.substring(1));

          if (await fileExists(actualFilePath)) {
            // Redirect to the actual file path
            return new Response(null, {
              status: 302,
              headers: {
                Location: actualPath,
                "Cache-Control": `public, max-age=${MAX_AGE}`,
              },
            });
          }
        }

        // Validate path structure
        const type = parts[1];
        const id = parts[2];
        const filename = parts[3];

        // For Property photos (Residential, Land, Commercial, Multi-Family)
        if (
          ["Residential", "Land", "Commercial", "Multi-Family"].includes(type)
        ) {
          if (parts.length !== 4) {
            return new Response("Invalid property photo path", { status: 400 });
          }
          const sizeAndId = filename;
          if (
            !["original", "large", "medium", "small", "thumb"].some((size) =>
              sizeAndId.startsWith(size)
            )
          ) {
            return new Response("Invalid image size", { status: 400 });
          }
        } else if (type !== "Agent" && type !== "Office") {
          return new Response("Invalid type", { status: 400 });
        }

        // Construct the full file path
        const filePath = path.join(CACHE_DIR, pathname.substring(1));

        // Check if file exists
        if (!(await fileExists(filePath))) {
          return new Response("Not Found", { status: 404 });
        }

        // Read the file
        const file = await fs.readFile(filePath);
        const lastModified = await getLastModified(filePath);

        // Determine content type
        const fileType = await fileTypeFromBuffer(file);
        const contentType =
          fileType?.mime || getContentType(path.extname(filePath).slice(1));

        // Create response with caching headers
        const headers = new Headers({
          "Content-Type": contentType,
          "Cache-Control": `public, max-age=${MAX_AGE}`,
          "Last-Modified": lastModified.toUTCString(),
        });

        // Handle If-Modified-Since header
        const ifModifiedSince = req.headers.get("If-Modified-Since");
        if (ifModifiedSince) {
          const ifModifiedSinceDate = new Date(ifModifiedSince);
          if (lastModified <= ifModifiedSinceDate) {
            return new Response(null, { status: 304, headers });
          }
        }

        return new Response(file, { headers });
      } catch (error) {
        console.error("Error serving file:", error);
        return new Response("Internal Server Error", { status: 500 });
      }
    },
  });

  console.log(`Photo server running at http://localhost:${PORT}`);
  return server;
}
