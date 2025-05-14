CREATE TABLE IF NOT EXISTS `PhotoProcessing` (
  `ListingID` VARCHAR(50) NOT NULL,
  `PropertyType` VARCHAR(10) NOT NULL,
  `Status` ENUM('processing', 'completed', 'failed') NOT NULL DEFAULT 'processing',
  `LastProcessed` DATETIME NOT NULL,
  `needsReprocessing` BOOLEAN NOT NULL DEFAULT FALSE,
  `RetryCount` INT NOT NULL DEFAULT 0,
  `ErrorMessage` TEXT,
  `PhotoData` JSON,
  PRIMARY KEY (`ListingID`, `PropertyType`),
  INDEX `idx_status` (`Status`),
  INDEX `idx_last_processed` (`LastProcessed`),
  INDEX `idx_needs_reprocessing` (`needsReprocessing`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci; 