from enum import StrEnum


class MongoDBRoles(StrEnum):
    READ = "read"
    READ_WRITE = "readWrite"
    DB_ADMIN = "dbAdmin"
    DB_OWNER = "dbOwner"
    USER_ADMIN = "userAdmin"
    CLUSTER_ADMIN = "clusterAdmin"
    CLUSTER_MANAGER = "clusterManager"
    CLUSTER_MONITOR = "clusterMonitor"
    HOST_MANAGER = "hostManager"
    BACKUP = "backup"
    RESTORE = "restore"
    READ_ANY_DATABASE = "readAnyDatabase"
    READ_WRITE_ANY_DATABASE = "readWriteAnyDatabase"
    USER_ADMIN_ANY_DATABASE = "userAdminAnyDatabase"
    DB_ADMIN_ANY_DATABASE = "dbAdminAnyDatabase"
    ROOT = "root"