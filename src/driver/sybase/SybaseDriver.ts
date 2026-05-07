import type { ObjectLiteral } from "../../common/ObjectLiteral"
import type { DataSource } from "../../data-source"
import { ConnectionIsNotSetError } from "../../error/ConnectionIsNotSetError"
import { DriverPackageNotInstalledError } from "../../error/DriverPackageNotInstalledError"
import type { ColumnMetadata } from "../../metadata/ColumnMetadata"
import type { EntityMetadata } from "../../metadata/EntityMetadata"
import { PlatformTools } from "../../platform/PlatformTools"
import { RdbmsSchemaBuilder } from "../../schema-builder/RdbmsSchemaBuilder"
import type { Table } from "../../schema-builder/table/Table"
import type { TableColumn } from "../../schema-builder/table/TableColumn"
import type { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import type { View } from "../../schema-builder/view/View"
import { ApplyValueTransformers } from "../../util/ApplyValueTransformers"
import { DateUtils } from "../../util/DateUtils"
import { InstanceChecker } from "../../util/InstanceChecker"
import { OrmUtils } from "../../util/OrmUtils"
import type { Driver } from "../Driver"
import type { ColumnType } from "../types/ColumnTypes"
import type { CteCapabilities } from "../types/CteCapabilities"
import type { DataTypeDefaults } from "../types/DataTypeDefaults"
import type { MappedColumnTypes } from "../types/MappedColumnTypes"
import type { ReplicationMode } from "../types/ReplicationMode"
import type { IsolationLevel } from "../types/IsolationLevel"
import type { UpsertType } from "../types/UpsertType"
import type { SybaseConnectionOptions } from "./SybaseConnectionOptions"
import { SybaseQueryRunner } from "./SybaseQueryRunner"

/**
 * Organizes communication with Sybase ASE DBMS via the sybase-tds TDS 5.0 driver.
 */
export class SybaseDriver implements Driver {
    // -------------------------------------------------------------------------
    // Static Properties
    // -------------------------------------------------------------------------

    static readonly supportedIsolationLevels: IsolationLevel[] = [
        "READ UNCOMMITTED",
        "READ COMMITTED",
        "REPEATABLE READ",
        "SERIALIZABLE",
    ]

    // -------------------------------------------------------------------------
    // Public Properties
    // -------------------------------------------------------------------------

    /**
     * DataSource used by the driver.
     */
    dataSource: DataSource

    /**
     * Isolation levels supported by this driver.
     */
    supportedIsolationLevels = SybaseDriver.supportedIsolationLevels

    /**
     * Underlying sybase-tds module instance.
     */
    sybase: any

    /**
     * Connection pool (sybase-tds ConnectionPool).
     */
    pool: any

    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    /**
     * DataSource options.
     */
    get options(): SybaseConnectionOptions {
        return this.dataSource.options as SybaseConnectionOptions
    }

    /**
     * Master database used to perform all write queries.
     */
    database?: string

    /**
     * Schema is not used in Sybase ASE; always undefined.
     */
    schema?: string

    /**
     * Replication is not supported.
     */
    isReplicated = false

    /**
     * Tree tables require CTE support, which Sybase ASE does not have.
     */
    treeSupport = false

    /**
     * Sybase ASE does not support savepoints in TDS 5.0.
     */
    transactionSupport = "simple" as const

    /**
     * Sybase ASE does not have a native UPSERT / MERGE statement in TDS 5.0.
     */
    supportedUpsertTypes: UpsertType[] = []

    /**
     * CTE is not supported in Sybase ASE.
     */
    cteCapabilities: CteCapabilities = {
        enabled: false,
    }

    maxAliasLength = 28

    /**
     * Supported column data types by Sybase ASE.
     */
    supportedDataTypes: ColumnType[] = [
        "int",
        "bigint",
        "smallint",
        "tinyint",
        "float",
        "real",
        "decimal",
        "numeric",
        "money",
        "smallmoney",
        "bit",
        "char",
        "varchar",
        "text",
        "nchar",
        "nvarchar",
        "binary",
        "varbinary",
        "image",
        "date",
        "time",
        "datetime",
        "smalldatetime",
        "timestamp",
        "simple-array",
        "simple-json",
        "simple-enum",
    ]

    /**
     * No spatial types in Sybase ASE.
     */
    spatialTypes: ColumnType[] = []

    /**
     * Column types that support a length specifier.
     */
    withLengthColumnTypes: ColumnType[] = [
        "char",
        "varchar",
        "nchar",
        "nvarchar",
        "binary",
        "varbinary",
    ]

    /**
     * Column types that support a precision specifier.
     */
    withPrecisionColumnTypes: ColumnType[] = ["decimal", "numeric"]

    /**
     * Column types that support a scale specifier.
     */
    withScaleColumnTypes: ColumnType[] = ["decimal", "numeric"]

    /**
     * Default column lengths / precision / scale for specific types.
     */
    dataTypeDefaults: DataTypeDefaults = {
        varchar: { length: 255 },
        nvarchar: { length: 255 },
        char: { length: 1 },
        nchar: { length: 1 },
        binary: { length: 1 },
        varbinary: { length: 1 },
        decimal: { precision: 18, scale: 0 },
        numeric: { precision: 18, scale: 0 },
    }

    /**
     * Mapping of TypeORM special column types to Sybase ASE column types.
     */
    mappedDataTypes: MappedColumnTypes = {
        createDate: "datetime",
        createDateDefault: "getdate()",
        updateDate: "datetime",
        updateDateDefault: "getdate()",
        deleteDate: "datetime",
        deleteDateNullable: true,
        version: "int",
        treeLevel: "int",
        migrationId: "int",
        migrationName: "varchar",
        migrationTimestamp: "bigint",
        cacheId: "int",
        cacheIdentifier: "varchar",
        cacheTime: "bigint",
        cacheDuration: "int",
        cacheQuery: "text",
        cacheResult: "text",
        metadataType: "varchar",
        metadataDatabase: "varchar",
        metadataSchema: "varchar",
        metadataTable: "varchar",
        metadataName: "varchar",
        metadataValue: "text",
    }

    /**
     * The placeholder token for positional query parameters.
     */
    parametersPrefix: string = "?"

    // -------------------------------------------------------------------------
    // Public Implemented Methods (required by Driver interface)
    // -------------------------------------------------------------------------

    isReturningSqlSupported(): boolean {
        return false
    }

    isUUIDGenerationSupported(): boolean {
        return false
    }

    isFullTextColumnTypeSupported(): boolean {
        return false
    }

    createParameter(_parameterName: string, _index: number): string {
        return "?"
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(dataSource: DataSource) {
        this.dataSource = dataSource
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Performs connection to the database.
     * Creates a connection pool using sybase-tds.
     */
    async connect(): Promise<void> {
        this.sybase = this.options.driver || this.loadDependencies()

        const connectOptions: Record<string, any> = {
            host: this.options.host,
            port: this.options.port ?? 5000,
            username: this.options.username ?? "",
            password: this.options.password ?? "",
        }
        if (this.options.charset) connectOptions.charset = this.options.charset
        if (this.options.language)
            connectOptions.language = this.options.language
        if (this.options.packetSize)
            connectOptions.packetSize = this.options.packetSize

        const poolOptions = {
            min: this.options.pool?.min ?? 0,
            max: this.options.pool?.max ?? 10,
            idleTimeout: this.options.pool?.idleTimeout ?? 60_000,
        }

        this.pool = this.sybase.ConnectionPool.create(
            connectOptions,
            poolOptions,
        )
        this.database = this.options.database
    }

    /**
     * No post-connect work required for Sybase.
     */
    afterConnect(): Promise<void> {
        return Promise.resolve()
    }

    /**
     * Closes all connections in the pool.
     */
    async disconnect(): Promise<void> {
        if (this.pool) {
            await this.pool.end()
            this.pool = undefined
        }
    }

    /**
     * Creates a schema builder for database synchronization.
     */
    createSchemaBuilder() {
        return new RdbmsSchemaBuilder(this.dataSource)
    }

    /**
     * Creates a query runner for running queries.
     *
     * @param mode
     */
    createQueryRunner(mode: ReplicationMode) {
        return new SybaseQueryRunner(this, mode)
    }

    /**
     * Acquires a connection from the pool (master).
     */
    async obtainMasterConnection(): Promise<any> {
        if (!this.pool) throw new ConnectionIsNotSetError("sybase")
        return this.pool.acquire()
    }

    /**
     * No slave support; delegates to master.
     */
    async obtainSlaveConnection(): Promise<any> {
        return this.obtainMasterConnection()
    }

    /**
     * Replaces named `:param` placeholders in SQL with positional `?` tokens
     * and returns the ordered parameter array for sybase-tds prepared statements.
     *
     * Each occurrence of the same named parameter produces a separate `?` and
     * pushes the value again, which is correct for positional-parameter drivers.
     *
     * @param sql
     * @param parameters
     */
    escapeQueryWithParameters(
        sql: string,
        parameters: ObjectLiteral,
    ): [string, any[]] {
        const escapedParameters: any[] = []
        if (!parameters || !Object.keys(parameters).length)
            return [sql, escapedParameters]

        sql = sql.replaceAll(
            /:(\.\.\.)?([A-Za-z0-9_.]+)/g,
            (full, isArray: string, key: string): string => {
                if (!parameters.hasOwnProperty(key)) return full

                const value: any = parameters[key]

                if (isArray) {
                    return (value as any[])
                        .map((v: any) => {
                            escapedParameters.push(v)
                            return "?"
                        })
                        .join(", ")
                }

                if (typeof value === "function") return value()

                escapedParameters.push(value)
                return "?"
            },
        )

        return [sql, escapedParameters]
    }

    /**
     * Escapes an identifier (table or column name) with double quotes.
     *
     * @param columnName
     */
    escape(columnName: string): string {
        return `[${columnName.replaceAll("]", "]]")}]`
    }

    /**
     * Builds a fully qualified table name.
     * Sybase uses `database..tableName` (double-dot) for cross-database access.
     *
     * @param tableName
     * @param _schema
     * @param database
     */
    buildTableName(
        tableName: string,
        _schema?: string,
        database?: string,
    ): string {
        if (database) return `${database}..${tableName}`
        return tableName
    }

    /**
     * Parses a target table name (string, Table, View, …) into its components.
     * Understands "database..table" (Sybase double-dot notation).
     *
     * @param target
     */
    parseTableName(
        target: EntityMetadata | Table | View | TableForeignKey | string,
    ): { database?: string; schema?: string; tableName: string } {
        const driverDatabase = this.database

        if (InstanceChecker.isTable(target) || InstanceChecker.isView(target)) {
            const parsed = this.parseTableName(target.name)
            return {
                database: target.database ?? parsed.database ?? driverDatabase,
                schema: undefined,
                tableName: parsed.tableName,
            }
        }

        if (InstanceChecker.isTableForeignKey(target)) {
            const parsed = this.parseTableName(target.referencedTableName)
            return {
                database:
                    target.referencedDatabase ??
                    parsed.database ??
                    driverDatabase,
                schema: undefined,
                tableName: parsed.tableName,
            }
        }

        if (InstanceChecker.isEntityMetadata(target)) {
            return {
                database: target.database ?? driverDatabase,
                schema: undefined,
                tableName: target.tableName,
            }
        }

        // "database..table" → { database, tableName }
        const doubleDotIdx = target.indexOf("..")
        if (doubleDotIdx !== -1) {
            return {
                database: target.slice(0, doubleDotIdx) || driverDatabase,
                schema: undefined,
                tableName: target.slice(doubleDotIdx + 2),
            }
        }

        return {
            database: driverDatabase,
            schema: undefined,
            tableName: target,
        }
    }

    /**
     * Converts a value to the format expected by Sybase for persistence.
     *
     * @param value
     * @param columnMetadata
     */
    preparePersistentValue(value: any, columnMetadata: ColumnMetadata): any {
        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformTo(
                columnMetadata.transformer,
                value,
            )

        if (value === null || value === undefined) return value

        if (columnMetadata.type === Boolean) {
            return value === true ? 1 : 0
        } else if (columnMetadata.type === "date") {
            return DateUtils.mixedDateToDate(value, columnMetadata.utc)
        } else if (columnMetadata.type === "time") {
            return DateUtils.mixedTimeToDate(value)
        } else if (
            columnMetadata.type === "datetime" ||
            columnMetadata.type === "smalldatetime" ||
            columnMetadata.type === Date
        ) {
            return DateUtils.mixedDateToDate(value, false, false)
        } else if (columnMetadata.type === "simple-array") {
            return DateUtils.simpleArrayToString(value)
        } else if (columnMetadata.type === "simple-json") {
            return DateUtils.simpleJsonToString(value)
        } else if (columnMetadata.type === "simple-enum") {
            return DateUtils.simpleEnumToString(value)
        }

        return value
    }

    /**
     * Converts a value returned from Sybase into the JS type expected by TypeORM.
     *
     * @param value
     * @param columnMetadata
     */
    prepareHydratedValue(value: any, columnMetadata: ColumnMetadata): any {
        if (value === null || value === undefined)
            return columnMetadata.transformer
                ? ApplyValueTransformers.transformFrom(
                      columnMetadata.transformer,
                      value,
                  )
                : value

        if (columnMetadata.type === Boolean) {
            value = value ? true : false
        } else if (
            columnMetadata.type === "datetime" ||
            columnMetadata.type === "smalldatetime" ||
            columnMetadata.type === Date
        ) {
            value = DateUtils.normalizeHydratedDate(value)
        } else if (columnMetadata.type === "date") {
            value = DateUtils.mixedDateToDateString(value, {
                utc: columnMetadata.utc,
            })
        } else if (columnMetadata.type === "time") {
            value = DateUtils.mixedTimeToString(value)
        } else if (columnMetadata.type === "simple-array") {
            value = DateUtils.stringToSimpleArray(value)
        } else if (columnMetadata.type === "simple-json") {
            value = DateUtils.stringToSimpleJson(value)
        } else if (columnMetadata.type === "simple-enum") {
            value = DateUtils.stringToSimpleEnum(value, columnMetadata)
        } else if (columnMetadata.type === Number) {
            value = !isNaN(+value) ? parseInt(value) : value
        }

        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformFrom(
                columnMetadata.transformer,
                value,
            )

        return value
    }

    /**
     * Maps a TypeORM column type to the Sybase ASE SQL type name.
     *
     * @param column
     * @param column.type
     * @param column.length
     * @param column.precision
     * @param column.scale
     */
    normalizeType(column: {
        type?: ColumnType
        length?: number | string
        precision?: number | null
        scale?: number
    }): string {
        if (column.type === Number || column.type === "integer") {
            return "int"
        } else if (column.type === String) {
            return "varchar"
        } else if (column.type === Date) {
            return "datetime"
        } else if (column.type === Boolean) {
            return "bit"
        } else if (
            typeof column.type === "function" &&
            column.type.prototype instanceof Uint8Array
        ) {
            return "binary"
        } else if (column.type === "uuid") {
            // Sybase has no native UUID type; store as char(36)
            return "char"
        } else if (
            column.type === "simple-array" ||
            column.type === "simple-json"
        ) {
            return "text"
        } else if (column.type === "simple-enum") {
            return "varchar"
        } else if (column.type === "dec") {
            return "decimal"
        } else if (
            column.type === "double" ||
            column.type === "double precision"
        ) {
            return "float"
        } else {
            return (column.type as string) || ""
        }
    }

    /**
     * Normalizes the default value for a column.
     *
     * @param columnMetadata
     */
    normalizeDefault(columnMetadata: ColumnMetadata): string | undefined {
        const defaultValue = columnMetadata.default

        if (typeof defaultValue === "number") {
            return `${defaultValue}`
        }

        if (typeof defaultValue === "boolean") {
            return defaultValue ? "1" : "0"
        }

        if (typeof defaultValue === "function") {
            const value = defaultValue()
            if (value.toUpperCase() === "CURRENT_TIMESTAMP") {
                return "getdate()"
            }
            return value
        }

        if (typeof defaultValue === "string") {
            return `'${defaultValue}'`
        }

        if (defaultValue === undefined || defaultValue === null) {
            return undefined
        }

        return `${defaultValue}`
    }

    /**
     * Returns whether the column should be treated as unique
     * based on its entity metadata.
     *
     * @param column
     */
    normalizeIsUnique(column: ColumnMetadata): boolean {
        return column.entityMetadata.uniques.some(
            (uq) => uq.columns.length === 1 && uq.columns[0] === column,
        )
    }

    /**
     * Returns the column length string, applying defaults for types that have them.
     *
     * @param column
     */
    getColumnLength(column: ColumnMetadata | TableColumn): string {
        if (column.length) return column.length.toString()

        if (
            column.type === "varchar" ||
            column.type === "nvarchar" ||
            column.type === String
        )
            return "255"

        if (column.type === "uuid") return "36"

        return ""
    }

    /**
     * Builds the full SQL type string including length/precision/scale.
     *
     * @param column
     */
    createFullType(column: TableColumn): string {
        if (column.asExpression) return ""

        let type = column.type

        if (this.getColumnLength(column)) {
            type += `(${this.getColumnLength(column)})`
        } else if (
            column.precision !== null &&
            column.precision !== undefined &&
            column.scale !== null &&
            column.scale !== undefined
        ) {
            type += `(${column.precision},${column.scale})`
        } else if (
            column.precision !== null &&
            column.precision !== undefined
        ) {
            type += `(${column.precision})`
        }

        return type
    }

    /**
     * Creates a generated map of identity values returned after INSERT.
     *
     * @param metadata
     * @param insertResult
     */
    createGeneratedMap(
        metadata: EntityMetadata,
        insertResult: ObjectLiteral,
    ): ObjectLiteral | undefined {
        if (!insertResult) return undefined

        return Object.keys(insertResult).reduce((map, key) => {
            const column = metadata.findColumnWithDatabaseName(key)
            if (column) {
                OrmUtils.mergeDeep(
                    map,
                    column.createValueMap(
                        this.prepareHydratedValue(insertResult[key], column),
                    ),
                )
            }
            return map
        }, {} as ObjectLiteral)
    }

    /**
     * Finds columns that have changed between the schema and entity metadata.
     *
     * @param tableColumns
     * @param columnMetadatas
     */
    findChangedColumns(
        tableColumns: TableColumn[],
        columnMetadatas: ColumnMetadata[],
    ): ColumnMetadata[] {
        return columnMetadatas.filter((columnMetadata) => {
            const tableColumn = tableColumns.find(
                (c) => c.name === columnMetadata.databaseName,
            )
            if (!tableColumn) return false

            return (
                tableColumn.name !== columnMetadata.databaseName ||
                tableColumn.type !==
                    this.normalizeType(columnMetadata).toLowerCase() ||
                tableColumn.length !== columnMetadata.length?.toString() ||
                tableColumn.precision !== columnMetadata.precision ||
                tableColumn.scale !== columnMetadata.scale ||
                tableColumn.isGenerated !== columnMetadata.isGenerated ||
                (!tableColumn.isGenerated &&
                    this.normalizeDefault(columnMetadata) !==
                        tableColumn.default) ||
                tableColumn.isPrimary !== columnMetadata.isPrimary ||
                tableColumn.isNullable !== columnMetadata.isNullable ||
                tableColumn.isUnique !==
                    this.normalizeIsUnique(columnMetadata) ||
                (tableColumn.enum &&
                    columnMetadata.enum &&
                    !OrmUtils.isArraysEqual(
                        tableColumn.enum,
                        columnMetadata.enum.map((val) => val + ""),
                    ))
            )
        })
    }

    /**
     * Returns true if the given length is the default for the column type.
     *
     * @param _table
     * @param column
     * @param length
     */
    isDefaultColumnLength(
        _table: Table,
        column: TableColumn,
        length: string,
    ): boolean {
        if (this.dataTypeDefaults?.[column.type]?.length) {
            return (
                this.dataTypeDefaults[column.type]!.length!.toString() ===
                length
            )
        }
        return false
    }

    /**
     * Returns true if the given precision is the default for the column type.
     *
     * @param _table
     * @param column
     * @param precision
     */
    isDefaultColumnPrecision(
        _table: Table,
        column: TableColumn,
        precision: number,
    ): boolean {
        if (this.dataTypeDefaults?.[column.type]?.precision) {
            return this.dataTypeDefaults[column.type]!.precision! === precision
        }
        return false
    }

    /**
     * Returns true if the given scale is the default for the column type.
     *
     * @param _table
     * @param column
     * @param scale
     */
    isDefaultColumnScale(
        _table: Table,
        column: TableColumn,
        scale: number,
    ): boolean {
        if (this.dataTypeDefaults?.[column.type]?.scale) {
            return this.dataTypeDefaults[column.type]!.scale! === scale
        }
        return false
    }

    // -------------------------------------------------------------------------
    // Private Methods
    // -------------------------------------------------------------------------

    /**
     * Loads the sybase-tds package or throws a helpful error.
     */
    private loadDependencies(): any {
        try {
            return PlatformTools.load("sybase-tds")
        } catch {
            throw new DriverPackageNotInstalledError("Sybase", "sybase-tds")
        }
    }
}
