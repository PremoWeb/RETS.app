CREATE TABLE IF NOT EXISTS `Deleted_Property_visible` (
  `DeletedID` INT PRIMARY KEY COMMENT 'Deleted ID',
  `DeletedDate` DATETIME default '0000-00-00 00:00:00' NOT NULL COMMENT 'Deleted Date',
  `MLS` TEXT COMMENT 'MLS #'
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT 'Deleted Items - Properties (Visible Names)';