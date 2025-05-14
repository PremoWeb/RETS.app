import * as https from "https";
import { URL } from "url";
import { RetsParser } from "../rets/retsParser";
import {
  saveCapabilities,
  loadCapabilities,
  isCapabilitiesCacheValid,
  CAPABILITIES_CACHE_FILE,
} from "./capabilities";
import * as fs from "fs/promises";
import { retsConfig } from "../utils/config";
import { makeRetsRequest } from "../utils/http";

// Interface for session data
export interface RetsSession {
  sessionId: string;
  capabilityUrls: { [key: string]: string };
}

// Helper function to build RETS URLs
function buildRetsUrl(relativeUrl: string): URL {
  const baseUrl = new URL(retsConfig.loginUrl!);
  // If the relative URL starts with a slash, remove it to avoid double slashes
  const cleanRelativeUrl = relativeUrl.startsWith("/")
    ? relativeUrl.substring(1)
    : relativeUrl;
  return new URL(cleanRelativeUrl, `${baseUrl.protocol}//${baseUrl.hostname}`);
}

// Function to perform Login
export async function login(): Promise<RetsSession> {
  // Check for cached capabilities and session first
  const cached = await loadCapabilities();
  if (cached && isCapabilitiesCacheValid(cached)) {
    return {
      sessionId: cached.sessionId,
      capabilityUrls: cached.capabilities,
    };
  }

  const url = buildRetsUrl(retsConfig.loginUrl!);
  const auth = Buffer.from(
    `${retsConfig.username}:${retsConfig.password}`
  ).toString("base64");

  const options: https.RequestOptions = {
    hostname: url.hostname,
    path: url.pathname + url.search + `?rets-version=${retsConfig.version}`,
    method: "GET",
    headers: {
      Authorization: `Basic ${auth}`,
      "User-Agent": retsConfig.userAgent!,
      "RETS-Version": retsConfig.version!,
    },
  };

  try {
    const { response, headers } = await makeRetsRequest(options);
    const responseText =
      typeof response === "string" ? response : response.toString("utf8");
    const parsed = await RetsParser.parse(responseText);

    if (parsed.ReplyCode !== "0") {
      throw new Error(`Login failed: ${parsed.ReplyText}`);
    }

    // Extract all cookies from the response
    const cookies = headers["set-cookie"];
    if (!cookies || cookies.length === 0) {
      throw new Error("No cookies received");
    }

    // Join all cookies into a single string
    const sessionId = cookies
      .map((cookie: string) => cookie.split(";")[0])
      .join("; ");

    // Parse capability URLs from RETS-RESPONSE
    const capabilityUrls = parsed["RETS-RESPONSE"] as { [key: string]: string };

    // Extract SearchMaxRows or MaxRows if present
    let maxRows = capabilityUrls["SearchMaxRows"] || capabilityUrls["MaxRows"];
    if (maxRows) {
      capabilityUrls["SearchMaxRows"] = maxRows;
    }

    // Calculate session expiration (default to 1 hour if not specified)
    const sessionExpires = Date.now() + 60 * 60 * 1000; // 1 hour from now

    // Cache the capabilities and session
    await saveCapabilities(capabilityUrls, sessionId, sessionExpires);

    console.log("Login successful. Session established.");
    return { sessionId, capabilityUrls };
  } catch (error) {
    throw new Error(
      `Login error: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

// Function to perform Logout
export async function logout(session: RetsSession): Promise<void> {
  const logoutUrl = session.capabilityUrls.Logout;
  if (!logoutUrl) {
    throw new Error("Logout URL not found in capability URLs");
  }

  const url = buildRetsUrl(logoutUrl);
  const options: https.RequestOptions = {
    hostname: url.hostname,
    path: url.pathname + (url.search || ""),
    method: "GET",
    headers: {
      "User-Agent": retsConfig.userAgent!,
      "RETS-Version": retsConfig.version!,
      Cookie: session.sessionId,
    },
  };

  try {
    const { response } = await makeRetsRequest(options);
    const responseText =
      typeof response === "string" ? response : response.toString("utf8");
    const parsed = await RetsParser.parse(responseText);

    if (parsed.ReplyCode !== "0") {
      throw new Error(`Logout failed: ${parsed.ReplyText}`);
    }

    // Clear the capabilities cache on successful logout
    try {
      await fs.unlink(CAPABILITIES_CACHE_FILE);
      console.log("Capabilities cache cleared on logout.");
    } catch (error) {
      console.error("Failed to clear capabilities cache:", error);
    }

    console.log("Logout successful. Session closed.");
  } catch (error) {
    throw new Error(
      `Logout error: ${error instanceof Error ? error.message : String(error)}`
    );
  }
}

// Function to create authenticated request options
export function createAuthenticatedOptions(
  session: RetsSession,
  relativeUrl: string,
  queryParams?: URLSearchParams
): https.RequestOptions {
  const auth = Buffer.from(
    `${retsConfig.username}:${retsConfig.password}`
  ).toString("base64");
  const url = buildRetsUrl(relativeUrl);

  return {
    hostname: url.hostname,
    path:
      url.pathname +
      (queryParams ? `?${queryParams.toString()}` : url.search || ""),
    method: "GET",
    headers: {
      "User-Agent": retsConfig.userAgent!,
      "RETS-Version": retsConfig.version!,
      Cookie: session.sessionId,
      Authorization: `Basic ${auth}`,
    },
  };
}
