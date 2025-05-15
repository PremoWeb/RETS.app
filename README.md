# RETS.app

Copyright (C) 2024 PremoWeb LLC

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License v3 for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

## Professional Setup & Customization

While this software is open source and free to use, professional setup and customization services are available:

- Rate: $125/hour
- Provider: Nick Maietta <nick@rets.app> or call +1-855-624-3882
- Billing: PremoWeb LLC
- Payment: Upfront payment required for estimated time. Quotes are free.

### Complete Realty Website Solution

A full-featured SvelteKit realty website is available for purchase and customization. This modern, high-performance website integrates seamlessly with this RETS data service. Contact Nick Maietta for pricing and customization options. Volume discounts available for multiple sites.

### Why Open Source?

This software was released as open source after a complete rewrite from the ground up. The previous version was developed in partnership, but due to payment collection issues, a new version was created independently. This new version is entirely separate from the previous partnership, allowing it to be released under the GPL v3 license.

# RETS Data Service

This service manages the synchronization of real estate data and photos from RETS servers.

## Primary Services

### Data Sync Service (`service.ts`)

The main data synchronization service that runs continuously and handles:

- Automatic data synchronization from RETS servers
- Database updates and maintenance
- Table creation and schema management
- Background photo processing and optimization

#### Update Schedule

- Active listings: Updated every minute
- Sold listings: Updated every 3 hours (at 3, 6, 9, 12 hour intervals)
- Expired/Withdrawn listings: Removed every 3 hours (at 3, 6, 9, 12 hour intervals)

To start the data sync service:

```bash
bun run service.ts
```

### Photo Processing Service (`lib/rets/photoProcessingService.ts`)

An integrated service that automatically processes photos in the background:

- Processes property photos in parallel
- Optimizes images for different sizes (original, large, medium, small, thumb)
- Manages photo processing state in the database
- Handles retries and error recovery

#### Processing Priority

1. New/Updated listings (immediate processing)
2. Active listings
3. Sold listings

Photos are processed in the background and uploaded to Object Storage according to the priority order above. No manual intervention required - the service automatically manages the entire photo processing pipeline.

## Manual Scripts

The following scripts are available for manual operations:

### Photo Management

- `getPhotos.ts` - Fetch photos for a specific listing, agent, or office

  ```bash
  bun run getPhotos.ts <type> <id>
  # Example: bun run getPhotos.ts Property 230475
  # Example: bun run getPhotos.ts Agent 42
  # Example: bun run getPhotos.ts Office 5
  ```

- `fetchAgentOfficePhotos.ts` - Batch fetch photos for agents and offices
  ```bash
  bun run fetchAgentOfficePhotos.ts
  ```

### Data Management

- `syncLookups.ts` - Synchronize lookup tables and reference data

  ```bash
  bun run syncLookups.ts
  ```

  This script maintains essential lookup tables that power front-end functionality:

  - Property types and styles
  - Cities, neighborhoods, and school districts
  - Agent and office information
  - Property features and amenities
  - Status codes and listing types

  These lookup tables are crucial for:

  - Search filters and faceted navigation
  - Form dropdowns and selection menus
  - Data validation and standardization
  - Geographic search capabilities
  - Property categorization and organization

  Run this script after initial setup and whenever you need to refresh reference data.

## Directory Structure

- `lib/` - Core library code

  - `rets/` - RETS-specific functionality
    - `photoProcessingService.ts` - Photo processing service
    - `photoWorker.ts` - Worker thread for parallel photo processing
    - `propertyPhotos.ts` - Core photo processing functionality
  - `db/` - Database operations
  - `auth/` - Authentication and session management
  - `utils/` - Utility functions

- `scripts/` - Utility scripts

  - `syncPhotosToObjectStorage.ts` - Sync photos to object storage

- `cache/` - Local cache directory for photos and temporary data

## Requirements

- Bun runtime
- MySQL database
- RETS server credentials
- S3-compatible object storage provider (e.g., Vultr Object Storage, AWS S3, etc.)
  - Access key
  - Secret key
  - Bucket name
  - Endpoint URL
