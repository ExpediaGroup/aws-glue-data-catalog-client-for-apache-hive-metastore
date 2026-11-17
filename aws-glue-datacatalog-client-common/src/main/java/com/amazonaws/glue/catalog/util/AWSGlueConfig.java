package com.amazonaws.glue.catalog.util;

import com.amazonaws.ClientConfiguration;

public final class AWSGlueConfig {

    private AWSGlueConfig() {}

    public static final String AWS_GLUE_ENDPOINT = "aws.glue.endpoint";
    public static final String AWS_REGION = "aws.region";
    public static final String AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS
            = "aws.catalog.credentials.provider.factory.class";

    public static final String AWS_GLUE_MAX_RETRY = "aws.glue.max-error-retries";
    public static final int DEFAULT_MAX_RETRY = 5;

    public static final String AWS_GLUE_MAX_CONNECTIONS = "aws.glue.max-connections";
    public static final int DEFAULT_MAX_CONNECTIONS = ClientConfiguration.DEFAULT_MAX_CONNECTIONS;

    public static final String AWS_GLUE_CONNECTION_TIMEOUT = "aws.glue.connection-timeout";
    public static final int DEFAULT_CONNECTION_TIMEOUT = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;

    public static final String AWS_GLUE_SOCKET_TIMEOUT = "aws.glue.socket-timeout";
    public static final int DEFAULT_SOCKET_TIMEOUT = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;

    public static final String AWS_GLUE_CATALOG_SEPARATOR = "aws.glue.catalog.separator";

    public static final String AWS_GLUE_DISABLE_UDF = "aws.glue.disable-udf";

    /**
     * When true, an AccessDeniedException whose message contains "Lake Formation" is translated
     * to NoSuchObjectException (for single-object reads) or treated as not-found (for existence
     * checks). This is an opt-in workaround for deployments where Lake Formation returns
     * AccessDeniedException instead of EntityNotFoundException for resources that do not exist,
     * which happens when the caller lacks CREATE_TABLE permission.
     *
     * WARNING: Lake Formation intentionally makes "does not exist" and "permission denied"
     * indistinguishable (anti-enumeration). Enabling this flag means genuine permission errors
     * will also be silently converted to not-found responses. Every translation is logged at WARN.
     * Default: false.
     */
    public static final String AWS_GLUE_LAKEFORMATION_ACCESS_DENIED_AS_NOT_FOUND =
            "aws.glue.lakeformation.access-denied-as-not-found";

    /**
     * When true, the Hive 3 client answers get_fields/get_schema for an Avro table with avro.schema.url or
     * avro.schema.literal from that schema, as Hive's metastore does, instead of from the columns stored in Glue.
     * A schema URL is read on every call, so the process needs a filesystem for its scheme and read access to it.
     * If the schema can't be read, the stored columns are returned and a warning is logged. Default: false.
     */
    public static final String AWS_GLUE_AVRO_SCHEMA_FROM_URL_ENABLE = "aws.glue.avro.schema-from-url.enable";

    /**
     * When true, alterTable renames Iceberg tables by creating the new Glue table and deleting the old one, as
     * Iceberg's GlueCatalog does. Not atomic: see GlueMetastoreClientDelegate#renameIcebergTable. Hive tables are
     * still refused. When false, any rename throws UnsupportedOperationException as before. Default: false.
     */
    public static final String AWS_GLUE_ICEBERG_RENAME_ENABLE = "aws.glue.iceberg.rename.enable";

    /**
     * When true, getTable leaves out the columns of an Iceberg table whose iceberg.field.current parameter is
     * "false". Iceberg's GlueCatalog writes the columns of every schema in the table's history and marks the
     * dropped ones this way, but Hive's FieldSchema has no parameters, so without this a Hive client sees dropped
     * columns as live ones, and can see two columns with the same name. Hive's metastore only ever held the
     * current schema's columns. Columns without the parameter, and non-Iceberg tables, are unchanged.
     * Default: false.
     */
    public static final String AWS_GLUE_ICEBERG_HIDE_NON_CURRENT_COLUMNS =
            "aws.glue.iceberg.hide-non-current-columns.enable";


    public static final String AWS_GLUE_DB_CACHE_ENABLE = "aws.glue.cache.db.enable";
    public static final String AWS_GLUE_DB_CACHE_SIZE = "aws.glue.cache.db.size";
    public static final String AWS_GLUE_DB_CACHE_TTL_MINS = "aws.glue.cache.db.ttl-mins";

    public static final String AWS_GLUE_TABLE_CACHE_ENABLE = "aws.glue.cache.table.enable";
    public static final String AWS_GLUE_TABLE_CACHE_SIZE = "aws.glue.cache.table.size";
    public static final String AWS_GLUE_TABLE_CACHE_TTL_MINS = "aws.glue.cache.table.ttl-mins";
}