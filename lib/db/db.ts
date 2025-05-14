import {
  createPool,
  Pool,
  PoolConnection,
  RowDataPacket,
} from "mysql2/promise";
import { dbConfig } from "../utils/config";
import path from "path";
import fs from "fs/promises";
import mysql from "mysql2/promise";

const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY = 1000; // 1 second

// Create a connection pool
const pool = createPool({
  host: dbConfig.host,
  port: dbConfig.port,
  user: dbConfig.user,
  password: dbConfig.password,
  database: dbConfig.database,
  multipleStatements: true,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

async function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function getConnection(): Promise<PoolConnection> {
  return pool.getConnection();
}

export async function tableExists(
  connection: PoolConnection,
  table: string
): Promise<boolean> {
  try {
    const [rows] = await connection.query<RowDataPacket[]>(
      "SHOW TABLES LIKE ?",
      [table]
    );
    return rows.length > 0;
  } finally {
    connection.release();
  }
}

export async function getLatestUpdateValue(
  connection: PoolConnection,
  table: string,
  updateField: string
): Promise<string | null> {
  try {
    const [rows] = await connection.query<RowDataPacket[]>(
      "SELECT MAX(??) as last_update FROM ??",
      [updateField, table]
    );
    return rows[0]?.last_update || null;
  } catch (error) {
    return null;
  } finally {
    connection.release();
  }
}

export function buildUpsertSQL(
  table: string,
  record: any
): { sql: string; values: any[] } {
  const keys = Object.keys(record);
  const placeholders = keys.map(() => "?").join(",");
  const sql = `REPLACE INTO \`${table}\` (${keys
    .map((k) => `\`${k}\``)
    .join(",")}) VALUES (${placeholders})`;
  const values = keys.map((k) => record[k]);
  return { sql, values };
}

export function sanitizeRecord(record: any, tableMetadata: any[]): any {
  const sanitized: any = {};
  for (const key in record) {
    const value = record[key];
    const fieldMeta = tableMetadata.find((f: any) => f.SystemName === key);
    if (value === "" || value == null) {
      if (fieldMeta) {
        const type = fieldMeta.DataType?.toLowerCase();
        if (type === "date") {
          sanitized[key] = "0000-00-00";
        } else if (type === "datetime") {
          sanitized[key] = "0000-00-00 00:00:00";
        } else if (type === "time") {
          sanitized[key] = "00:00:00";
        } else {
          sanitized[key] = null;
        }
      } else {
        sanitized[key] = null;
      }
    } else {
      sanitized[key] = value;
    }
  }
  return sanitized;
}

export function formatRetsDate(date: string | Date | null): string | null {
  if (!date) return null;
  const d = typeof date === "string" ? new Date(date) : date;
  if (isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 19);
}

export async function applySchemas(): Promise<void> {
  const connection = await getConnection();
  const sqlDir = path.join(process.cwd(), "sql");
  const files = await fs.readdir(sqlDir);
  const sqlFiles = files.filter((f) => f.endsWith(".sql"));

  try {
    for (const file of sqlFiles) {
      const filePath = path.join(sqlDir, file);
      const sql = await fs.readFile(filePath, "utf8");
      console.log(`Applying schema: ${file}`);
      await connection.query(sql);
      console.log(`Applied: ${file}`);
    }
    console.log("All schemas applied successfully.");
  } catch (error) {
    console.error(
      "Error applying schemas:",
      error instanceof Error ? error.message : String(error)
    );
    process.exit(1);
  } finally {
    connection.release();
  }
}
