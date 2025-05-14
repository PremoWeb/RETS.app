import * as fs from 'fs/promises';
import * as path from 'path';

export const CAPABILITIES_CACHE_FILE = path.join(process.cwd(), 'cache', 'rets-capabilities.json');

// Ensure cache directory exists
const CACHE_DIR = path.join(process.cwd(), 'cache');
await fs.mkdir(CACHE_DIR, { recursive: true });

export interface RetsCapabilities {
  sessionId: string;
  sessionExpires: number;
  capabilities: { [key: string]: string };
}

export async function saveCapabilities(capabilities: { [key: string]: string }, sessionId: string, sessionExpires: number): Promise<void> {
  const data: RetsCapabilities = {
    sessionId,
    sessionExpires,
    capabilities
  };

  try {
    await fs.writeFile(CAPABILITIES_CACHE_FILE, JSON.stringify(data, null, 2));
    console.log('Capabilities and session cached successfully.');
  } catch (error) {
    console.error('Failed to cache capabilities:', error);
  }
}

export async function loadCapabilities(): Promise<RetsCapabilities | null> {
  try {
    const data = await fs.readFile(CAPABILITIES_CACHE_FILE, 'utf-8');
    return JSON.parse(data) as RetsCapabilities;
  } catch (error) {
    return null;
  }
}

export function isCapabilitiesCacheValid(cache: RetsCapabilities): boolean {
  const now = Date.now();
  return now < cache.sessionExpires;
}

export function getCapabilityUrl(cache: RetsCapabilities, capability: string): string | null {
  return cache.capabilities[capability] || null;
} 