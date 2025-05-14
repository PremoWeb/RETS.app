import * as dotenv from 'dotenv';

// Load environment variables
dotenv.config();

// Interface for RETS configuration
export interface RetsConfig {
  loginUrl: string;
  version: string;
  vendor: string;
  username: string;
  password: string;
  userAgent: string;
}

// Validate and export RETS configuration
export const retsConfig: RetsConfig = {
  loginUrl: process.env.RETS_LOGIN_URL!,
  version: process.env.RETS_VERSION!,
  vendor: process.env.RETS_VENDOR!,
  username: process.env.RETS_USERNAME!,
  password: process.env.RETS_PASSWORD!,
  userAgent: process.env.RETS_USER_AGENT!,
};

// Validate all required environment variables are present
for (const [key, value] of Object.entries(retsConfig)) {
  if (!value) {
    console.error(`Missing environment variable: ${key}`);
    process.exit(1);
  }
}

// Database configuration
export const dbConfig = {
  host: process.env.MYSQL_HOST || 'localhost',
  port: parseInt(process.env.MYSQL_PORT || '3306', 10),
  user: process.env.MYSQL_USER || 'rets_user',
  password: process.env.MYSQL_PASSWORD || 'rets_password',
  database: process.env.MYSQL_DATABASE || 'rets_data',
}; 