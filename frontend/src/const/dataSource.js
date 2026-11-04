export const MySQL = ['MySQL', 'GoldenDBMySQL', 'YashanDBMySQL'];
export const PostgreSQL = ['PostgreSQL'];
export const Greenplum = ['Greenplum'];
export const Cloudberry = ['Cloudberry'];
export const CockroachDB = ['CockroachDB'];
export const Oracle = ['Oracle', 'GoldenDBOracle', 'YashanDBOracle'];
export const Redis = ['Redis', 'Valkey'];
export const TiDB = ['TiDB'];

export const SqlServer = ['SQLServer'];

export const Db2 = ['Db2'];

export const Dameng = ['Dameng'];

export const HasSchema = [...Oracle, ...PostgreSQL, ...Greenplum, ...Cloudberry, ...CockroachDB, ...SqlServer];

export const isMySQL = (type) => MySQL.includes(type);
export const isPostgreSQL = (type) => PostgreSQL.includes(type);
export const isOracle = (type) => Oracle.includes(type);
export const isRedis = (type) => Redis.includes(type);
