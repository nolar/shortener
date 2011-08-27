SET NAMES utf8;

DROP TABLE IF EXISTS urls;
CREATE TABLE urls (
    `id`        varchar(100) not null,
    `host`      varchar(100) not null,
    `code`      varchar(100) not null,
    `url`       longtext not null default '',
    `created_ts`  float default null,
    `remote_addr` varchar(100) default null,
    `remote_port` varchar(10) default null,
    primary key(`host`, `id`)
);

DROP TABLE IF EXISTS last_urls;
CREATE TABLE last_urls (
    `id`        varchar(100) not null,
    `host`      varchar(100) not null,
    `code`      varchar(100) not null,
    `url`       longtext not null default '',
    `created_ts`  float default null,
    `remote_addr` varchar(100) default null,
    `remote_port` varchar(10) default null,
    `timestamp`  integer unsigned not null,
    index(`timestamp`),
    primary key(`host`, `id`)
);

DROP TABLE IF EXISTS sequences;
CREATE TABLE sequences (
    `id`        varchar(100) not null,
    `host`      varchar(100) not null,
    `value`      varchar(100) not null,
    primary key(`host`, `id`)
);
