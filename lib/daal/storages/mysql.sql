SET NAMES utf8;
USE `shortener`;

/*
 * Main storage for forward resolution of shortened urls to originals targets, plus meta info.
 * Code field is stored just for convenience and compatibility of algorithms with other storages,
 * who change id field automatically.
 */
DROP TABLE IF EXISTS `urls`;
CREATE TABLE `urls` (
    `host`              varchar(100) not null,
    `id`                varchar(100) not null,
    `code`              varchar(100) not null,
    `url`               longtext not null,
    `created_ts`        integer unsigned default null,
    `remote_addr`       varchar(45) default null, -- beware of IPv6, see http://stackoverflow.com/questions/166132/maximum-length-of-the-textual-representation-of-an-ipv6-address
    `remote_port`       smallint default null,
    primary key (`host`, `id`)
) engine=innodb;

/*
 * Same structure as with urls table, and the same item is stored.
 * But this table is cleaned regularly to keep the list short and sort it fast.
 * If LatestTargetsDimension will be refactored to key-value storages, this table will be removed.
 */
DROP TABLE IF EXISTS `last_urls`;
CREATE TABLE `last_urls` like `urls`;
ALTER TABLE `last_urls`
    ADD COLUMN `timestamp` integer unsigned not null,
    ADD INDEX (`timestamp`);

/*
 * Used by generators to store their state (current value).
 * Value has no default field as defined for counters, since it is not
 * a storage-level numeric counter, but is a generator-level textual one.
 */
DROP TABLE IF EXISTS `sequences`;
CREATE TABLE `sequences` (
    `host`              varchar(100) not null,
    `id`                varchar(100) not null,
    `value`             varchar(100) not null,
    primary key (`host`, `id`)
) engine=innodb;

/*
 * Used by PopularDomainsDimension to store per-domain counters.
 * Value field must have a default of 0 as defined for counters (see mysql.py).
 */
DROP TABLE IF EXISTS `popular_domain_counters`;
CREATE TABLE `popular_domain_counters` (
    `host`              varchar(100) not null,
    `time_shard`        integer unsigned not null,
    `domain`            varchar(100) not null, -- why 100 chars? much more!
    `value`             integer unsigned not null default 0,
    primary key (`host`, `time_shard`, `domain`)
) engine=innodb;

/*
 * Used by PopularDomainsDimension to store number of domains in each of the grid levels.
 * Value field must have a default of 0 as defined for counters (see mysql.py).
 */
DROP TABLE IF EXISTS `popular_grid_level_counters`;
CREATE TABLE `popular_grid_level_counters` (
    `host`              varchar(100) not null,
    `time_shard`        integer unsigned not null,
    `grid_level`        tinyint unsigned not null, -- having more than 255 grid levels is really a bad idea, even 25 is too many.
    `value`             integer unsigned not null default 0,
    primary key (`host`, `time_shard`, `grid_level`)
) engine=innodb;

/*
 * Used by PopularDomainsDimension to store list of domains for each of the grid levels.
 * Value field must have a default of '' as defined for accumulators (see mysql.py).
 */
DROP TABLE IF EXISTS `popular_grid_level_domains`;
CREATE TABLE `popular_grid_level_domains` (
    `host`              varchar(100) not null,
    `time_shard`        integer unsigned not null,
    `grid_level`        tinyint unsigned not null, -- having more than 255 grid levels is really a bad idea, even 25 is too many.
    `value`             longtext not null default '',
    primary key (`host`, `time_shard`, `grid_level`)
) engine=innodb;
