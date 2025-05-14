import * as fs from 'fs/promises';
import * as path from 'path';
import { generateUpdateFields } from './generateUpdateFields';

export const UPDATE_FIELDS_CACHE_FILE = path.join(process.cwd(), 'cache', 'update_fields.json');

// Ensure cache directory exists
const CACHE_DIR = path.join(process.cwd(), 'cache');
await fs.mkdir(CACHE_DIR, { recursive: true });

// Cache the update fields data in memory
let updateFieldsCache: any = null;

/**
 * Gets the update fields data, generating it if necessary
 * @returns Promise resolving to the update fields data
 */
export async function getUpdateFields(): Promise<any> {
  // Return cached data if available
  if (updateFieldsCache) {
    return updateFieldsCache;
  }

  try {
    // Try to read the file
    const data = await fs.readFile(UPDATE_FIELDS_CACHE_FILE, 'utf-8');
    updateFieldsCache = JSON.parse(data);
    return updateFieldsCache;
  } catch (error) {
    // If file doesn't exist or can't be read, generate it
    console.log('Update fields file not found or invalid. Generating...');
    await generateUpdateFields();
    
    // Read the newly generated file
    const data = await fs.readFile(UPDATE_FIELDS_CACHE_FILE, 'utf-8');
    updateFieldsCache = JSON.parse(data);
    return updateFieldsCache;
  }
}

/**
 * Clears the update fields cache
 */
export function clearUpdateFieldsCache(): void {
  updateFieldsCache = null;
} 