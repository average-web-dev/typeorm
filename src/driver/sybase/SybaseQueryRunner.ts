import { QueryFailedError } from "../../error/QueryFailedError"
import { QueryRunnerAlreadyReleasedError } from "../../error/QueryRunnerAlreadyReleasedError"
import { TransactionAlreadyStartedError } from "../../error/TransactionAlreadyStartedError"
import { TransactionNotStartedError } from "../../error/TransactionNotStartedError"
import { TypeORMError } from "../../error/TypeORMError"
import { BaseQueryRunner } from "../../query-runner/BaseQueryRunner"
import { QueryLock } from "../../query-runner/QueryLock"
import { QueryResult } from "../../query-runner/QueryResult"
import type { QueryRunner } from "../../query-runner/QueryRunner"
import { Table } from "../../schema-builder/table/Table"
import { TableCheck } from "../../schema-builder/table/TableCheck"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import type { TableExclusion } from "../../schema-builder/table/TableExclusion"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { TableIndex } from "../../schema-builder/table/TableIndex"
import { TableUnique } from "../../schema-builder/table/TableUnique"
import { View } from "../../schema-builder/view/View"
import { Broadcaster } from "../../subscriber/Broadcaster"
import { BroadcasterResult } from "../../subscriber/BroadcasterResult"
import { InstanceChecker } from "../../util/InstanceChecker"
import { OrmUtils } from "../../util/OrmUtils"
import { Query } from "../Query"
import type { ColumnType } from "../types/ColumnTypes"
import type { IsolationLevel } from "../types/IsolationLevel"
import { MetadataTableType } from "../types/MetadataTableType"
import type { ReplicationMode } from "../types/ReplicationMode"
import type { SybaseDriver } from "./SybaseDriver"

/**
 * Runs queries on a single Sybase ASE database connection acquired from the pool.
 */
export class SybaseQueryRunner extends BaseQueryRunner implements QueryRunner {
    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    driver: SybaseDriver

    // -------------------------------------------------------------------------
    // Private Properties
    // -------------------------------------------------------------------------

    private lock: QueryLock = new QueryLock()

    /** sybase-tds Transaction object; set while a transaction is active */
    private sybaseTransaction: any = null

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(driver: SybaseDriver, mode: ReplicationMode) {
        super()
        this.driver = driver
        this.connection = driver.dataSource
        this.broadcaster = new Broadcaster(this)
        this.mode = mode
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Acquires a connection from the pool on first use and holds it
     * for the lifetime of this query runner.
     */
    async connect(): Promise<any> {
        if (this.databaseConnection) return this.databaseConnection

        this.databaseConnection = await (this.mode === "slave"
            ? this.driver.obtainSlaveConnection()
            : this.driver.obtainMasterConnection())

        return this.databaseConnection
    }

    /**
     * Releases the connection back to the pool.
     */
    async release(): Promise<void> {
        this.isReleased = true

        if (this.databaseConnection) {
            this.driver.pool.release(this.databaseConnection)
            this.databaseConnection = undefined
        }
    }

    /**
     * Starts a database transaction.
     *
     * @param isolationLevel
     */
    async startTransaction(isolationLevel?: IsolationLevel): Promise<void> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        if (
            this.isTransactionActive &&
            this.driver.transactionSupport === "simple"
        )
            throw new TransactionAlreadyStartedError()

        await this.broadcaster.broadcast("BeforeTransactionStart")

        const conn = await this.connect()

        if (isolationLevel) {
            await this.query(
                `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`,
            )
        }

        this.sybaseTransaction = conn.transaction()
        await this.sybaseTransaction.begin()
        this.isTransactionActive = true

        await this.broadcaster.broadcast("AfterTransactionStart")
    }

    /**
     * Commits the active transaction.
     */
    async commitTransaction(): Promise<void> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        if (!this.isTransactionActive) throw new TransactionNotStartedError()

        await this.broadcaster.broadcast("BeforeTransactionCommit")

        await this.sybaseTransaction.commit()
        this.sybaseTransaction = null
        this.isTransactionActive = false

        await this.broadcaster.broadcast("AfterTransactionCommit")
    }

    /**
     * Rolls back the active transaction.
     */
    async rollbackTransaction(): Promise<void> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        if (!this.isTransactionActive) throw new TransactionNotStartedError()

        await this.broadcaster.broadcast("BeforeTransactionRollback")

        await this.sybaseTransaction.rollback()
        this.sybaseTransaction = null
        this.isTransactionActive = false

        await this.broadcaster.broadcast("AfterTransactionRollback")
    }

    /**
     * Executes a SQL query.
     *
     * Parameterized queries are sent as TDS 5.0 DYNAMIC (prepared) statements
     * to prevent SQL injection. Non-parameterized queries use a plain LANGUAGE
     * token for simplicity.
     *
     * @param query
     * @param parameters
     * @param useStructuredResult
     */
    async query(
        query: string,
        parameters?: any[],
        useStructuredResult = false,
    ): Promise<any> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        const release = await this.lock.acquire()

        const conn = await this.connect()
        const result = new QueryResult()

        this.driver.dataSource.logger.logQuery(query, parameters, this)
        await this.broadcaster.broadcast("BeforeQuery", query, parameters)

        const broadcasterResult = new BroadcasterResult()
        const maxQueryExecutionTime =
            this.driver.dataSource.options.maxQueryExecutionTime
        let queryStartTime: number | undefined

        try {
            queryStartTime = Date.now()

            let rawResult: { columns: any[]; rows: any[][]; rowCount: number }

            if (parameters?.length) {
                // Use server-side prepared statement (TDS 5.0 DYNAMIC tokens)
                const stmt = await conn.prepare(query)
                try {
                    rawResult = await stmt.execute(parameters)
                } finally {
                    await stmt.close().catch(() => undefined)
                }
            } else {
                rawResult = await conn.query(query)
            }

            const queryExecutionTime = Date.now() - queryStartTime!

            this.broadcaster.broadcastAfterQueryEvent(
                broadcasterResult,
                query,
                parameters,
                true,
                queryExecutionTime,
                rawResult,
                undefined,
            )

            if (
                maxQueryExecutionTime &&
                queryExecutionTime > maxQueryExecutionTime
            ) {
                this.driver.dataSource.logger.logQuerySlow(
                    queryExecutionTime,
                    query,
                    parameters,
                    this,
                )
            }

            // Convert row arrays to named-field objects
            if (rawResult.rows?.length > 0) {
                result.records = rawResult.rows.map((row) => {
                    const obj: Record<string, any> = {}
                    rawResult.columns.forEach((col, i) => {
                        obj[col.name] = row[i]
                    })
                    return obj
                })
            }
            result.affected = rawResult.rowCount
            result.raw = result.records

            // After INSERT, fetch the generated IDENTITY value via @@IDENTITY.
            // This mirrors how SAP HANA uses CURRENT_IDENTITY_VALUE().
            if (
                query.trimStart().slice(0, 11).toUpperCase() === "INSERT INTO"
            ) {
                const idResult = await conn.query("SELECT @@IDENTITY AS id")
                const id = idResult.rows[0]?.[0]
                if (id !== null && id !== undefined) {
                    result.raw = id
                    result.records = [{ id }]
                }
            }

            return useStructuredResult ? result : result.raw
        } catch (err) {
            const queryExecutionTime = queryStartTime
                ? Date.now() - queryStartTime
                : undefined

            if (
                maxQueryExecutionTime &&
                queryExecutionTime !== undefined &&
                queryExecutionTime > maxQueryExecutionTime
            ) {
                this.driver.dataSource.logger.logQuerySlow(
                    queryExecutionTime,
                    query,
                    parameters,
                    this,
                )
            }

            this.driver.dataSource.logger.logQueryError(
                err,
                query,
                parameters,
                this,
            )
            this.broadcaster.broadcastAfterQueryEvent(
                broadcasterResult,
                query,
                parameters,
                false,
                undefined,
                undefined,
                err,
            )

            throw new QueryFailedError(query, parameters, err as Error)
        } finally {
            await broadcasterResult.wait()
            release()
        }
    }

    /**
     * Streaming is not supported by sybase-tds.
     */
    stream(): never {
        throw new TypeORMError(
            "Streaming is not supported by the Sybase driver.",
        )
    }

    // -------------------------------------------------------------------------
    // Schema inspection
    // -------------------------------------------------------------------------

    async getDatabases(): Promise<string[]> {
        const result = await this.query(
            `SELECT name FROM master..sysdatabases ORDER BY name`,
        )
        return (result as any[]).map((r: any) => r["name"])
    }

    async getSchemas(): Promise<string[]> {
        return []
    }

    async hasDatabase(database: string): Promise<boolean> {
        const result = await this.query(
            `SELECT COUNT(*) AS cnt FROM master..sysdatabases WHERE name = ?`,
            [database],
        )
        return Number(result[0]["cnt"]) > 0
    }

    async getCurrentDatabase(): Promise<string> {
        const result = await this.query(`SELECT db_name() AS db_name`)
        return result[0]["db_name"]
    }

    async hasSchema(): Promise<boolean> {
        return false
    }

    async getCurrentSchema(): Promise<string> {
        return ""
    }

    async hasTable(tableOrName: Table | string): Promise<boolean> {
        const { tableName } = this.driver.parseTableName(tableOrName)
        const result = await this.query(
            `SELECT COUNT(*) AS cnt FROM sysobjects WHERE type = 'U' AND name = ?`,
            [tableName],
        )
        return Number(result[0]["cnt"]) > 0
    }

    async hasColumn(
        tableOrName: Table | string,
        columnName: string,
    ): Promise<boolean> {
        const { tableName } = this.driver.parseTableName(tableOrName)
        const result = await this.query(
            `SELECT COUNT(*) AS cnt FROM syscolumns c ` +
                `INNER JOIN sysobjects o ON o.id = c.id ` +
                `WHERE o.type = 'U' AND o.name = ? AND c.name = ?`,
            [tableName, columnName],
        )
        return Number(result[0]["cnt"]) > 0
    }

    // -------------------------------------------------------------------------
    // Database/Schema DDL – Sybase does not use schemas in the SQL Server sense
    // -------------------------------------------------------------------------

    async createDatabase(
        database: string,
        ifNotExists?: boolean,
    ): Promise<void> {
        const up = ifNotExists
            ? `IF NOT EXISTS (SELECT 1 FROM master..sysdatabases WHERE name = '${database.replaceAll(
                  "'",
                  "''",
              )}') ` + `CREATE DATABASE ${this.driver.escape(database)}`
            : `CREATE DATABASE ${this.driver.escape(database)}`
        const down = `DROP DATABASE ${this.driver.escape(database)}`
        await this.executeQueries(new Query(up), new Query(down))
    }

    async dropDatabase(database: string, ifExists?: boolean): Promise<void> {
        const up = ifExists
            ? `IF EXISTS (SELECT 1 FROM master..sysdatabases WHERE name = '${database.replaceAll(
                  "'",
                  "''",
              )}') ` + `DROP DATABASE ${this.driver.escape(database)}`
            : `DROP DATABASE ${this.driver.escape(database)}`
        const down = `CREATE DATABASE ${this.driver.escape(database)}`
        await this.executeQueries(new Query(up), new Query(down))
    }

    async createSchema(): Promise<void> {
        // Not applicable for Sybase ASE
    }

    async dropSchema(): Promise<void> {
        // Not applicable for Sybase ASE
    }

    // -------------------------------------------------------------------------
    // Table DDL
    // -------------------------------------------------------------------------

    async createTable(
        table: Table,
        ifNotExists = false,
        createForeignKeys = true,
        createIndices = true,
    ): Promise<void> {
        if (ifNotExists) {
            const exists = await this.hasTable(table)
            if (exists) return
        }

        const upQueries: Query[] = []
        const downQueries: Query[] = []

        upQueries.push(this.createTableSql(table, createForeignKeys))
        downQueries.push(this.dropTableSql(table))

        if (createForeignKeys) {
            table.foreignKeys.forEach((fk) =>
                downQueries.push(this.dropForeignKeySql(table, fk)),
            )
        }

        if (createIndices) {
            table.indices.forEach((index) => {
                index.name ??= this.connection.namingStrategy.indexName(
                    table,
                    index.columnNames,
                    index.where,
                )
                upQueries.push(this.createIndexSql(table, index))
                downQueries.push(this.dropIndexSql(table, index))
            })
        }

        await this.executeQueries(upQueries, downQueries)
    }

    async dropTable(
        tableOrName: Table | string,
        ifExists?: boolean,
        dropForeignKeys = true,
        dropIndices = true,
    ): Promise<void> {
        if (ifExists) {
            const exists = await this.hasTable(tableOrName)
            if (!exists) return
        }

        const createForeignKeys = dropForeignKeys
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        if (dropIndices) {
            table.indices.forEach((index) => {
                upQueries.push(this.dropIndexSql(table, index))
                downQueries.push(this.createIndexSql(table, index))
            })
        }

        if (dropForeignKeys) {
            table.foreignKeys.forEach((fk) =>
                upQueries.push(this.dropForeignKeySql(table, fk)),
            )
        }

        upQueries.push(this.dropTableSql(table))
        downQueries.push(this.createTableSql(table, createForeignKeys))

        await this.executeQueries(upQueries, downQueries)
    }

    async renameTable(
        oldTableOrName: Table | string,
        newTableName: string,
    ): Promise<void> {
        const oldTable = InstanceChecker.isTable(oldTableOrName)
            ? oldTableOrName
            : await this.getCachedTable(oldTableOrName)
        const newTable = oldTable.clone()
        newTable.name = newTableName

        const upQueries: Query[] = [
            new Query(
                `EXEC sp_rename '${this.getTablePath(
                    oldTable,
                )}', '${newTableName}'`,
            ),
        ]
        const downQueries: Query[] = [
            new Query(
                `EXEC sp_rename '${this.getTablePath(newTable)}', '${
                    oldTable.name
                }'`,
            ),
        ]

        // Rebuild FK names that reference the table path
        newTable.foreignKeys.forEach((fk) => {
            fk.referencedTableName = newTableName
        })

        // Rename any indexes (indexes in Sybase are table-scoped)
        newTable.indices.forEach((index) => {
            upQueries.push(
                new Query(
                    `EXEC sp_rename '${newTableName}.${index.name}', '${index.name}', 'INDEX'`,
                ),
            )
        })

        await this.executeQueries(upQueries, downQueries)
        this.replaceCachedTable(oldTable, newTable)
    }

    // -------------------------------------------------------------------------
    // Column DDL
    // -------------------------------------------------------------------------

    async addColumn(
        tableOrName: Table | string,
        column: TableColumn,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const clonedTable = table.clone()

        const upQueries: Query[] = []
        const downQueries: Query[] = []

        upQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    table,
                )} ADD ${this.buildCreateColumnSql(table, column)}`,
            ),
        )
        downQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(table)} DROP [${column.name}]`,
            ),
        )

        if (column.isPrimary) {
            const primaryColumns = clonedTable.primaryColumns
            if (primaryColumns.length > 0) {
                const pkName =
                    primaryColumns[0].primaryKeyConstraintName ??
                    this.connection.namingStrategy.primaryKeyName(
                        clonedTable,
                        primaryColumns.map((c) => c.name),
                    )
                upQueries.push(
                    new Query(
                        `ALTER TABLE ${this.escapePath(
                            table,
                        )} DROP CONSTRAINT [${pkName}]`,
                    ),
                )
                downQueries.push(
                    new Query(
                        `ALTER TABLE ${this.escapePath(
                            table,
                        )} ADD CONSTRAINT [${pkName}] PRIMARY KEY (${primaryColumns
                            .map((c) => `[${c.name}]`)
                            .join(", ")})`,
                    ),
                )
            }
            clonedTable.columns.filter((c) => c.isPrimary).push(column)
            const newPkName =
                column.primaryKeyConstraintName ??
                this.connection.namingStrategy.primaryKeyName(
                    clonedTable,
                    clonedTable.primaryColumns.map((c) => c.name),
                )
            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        table,
                    )} ADD CONSTRAINT [${newPkName}] PRIMARY KEY (${clonedTable.primaryColumns
                        .map((c) => `[${c.name}]`)
                        .join(", ")})`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        table,
                    )} DROP CONSTRAINT [${newPkName}]`,
                ),
            )
        }

        if (column.isUnique) {
            const uniqueConstraint = new TableUnique({
                name: this.connection.namingStrategy.uniqueConstraintName(
                    table,
                    [column.name],
                ),
                columnNames: [column.name],
            })
            clonedTable.uniques.push(uniqueConstraint)
            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(table)} ADD CONSTRAINT [${
                        uniqueConstraint.name
                    }] UNIQUE ([${column.name}])`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT [${
                        uniqueConstraint.name
                    }]`,
                ),
            )
        }

        await this.executeQueries(upQueries, downQueries)
        clonedTable.addColumn(column)
        this.replaceCachedTable(table, clonedTable)
    }

    async addColumns(
        tableOrName: Table | string,
        columns: TableColumn[],
    ): Promise<void> {
        for (const column of columns) {
            await this.addColumn(tableOrName, column)
        }
    }

    async renameColumn(
        tableOrName: Table | string,
        oldTableColumnOrName: TableColumn | string,
        newTableColumnOrName: TableColumn | string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const oldColumn = InstanceChecker.isTableColumn(oldTableColumnOrName)
            ? oldTableColumnOrName
            : table.columns.find((c) => c.name === oldTableColumnOrName)
        if (!oldColumn)
            throw new TypeORMError(
                `Column [${oldTableColumnOrName}] was not found in the [${table.name}] table.`,
            )

        let newColumn: TableColumn
        if (InstanceChecker.isTableColumn(newTableColumnOrName)) {
            newColumn = newTableColumnOrName
        } else {
            newColumn = oldColumn.clone()
            newColumn.name = newTableColumnOrName
        }

        await this.changeColumn(table, oldColumn, newColumn)
    }

    async changeColumn(
        tableOrName: Table | string,
        oldTableColumnOrName: TableColumn | string,
        newColumn: TableColumn,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        let clonedTable = table.clone()
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        const oldColumn = InstanceChecker.isTableColumn(oldTableColumnOrName)
            ? oldTableColumnOrName
            : table.columns.find((c) => c.name === oldTableColumnOrName)
        if (!oldColumn)
            throw new TypeORMError(
                `Column [${oldTableColumnOrName}] was not found in the [${table.name}] table.`,
            )

        if (
            newColumn.type !== oldColumn.type ||
            newColumn.length !== oldColumn.length ||
            newColumn.isGenerated !== oldColumn.isGenerated
        ) {
            // Sybase ASE does not support in-place type change: drop + add
            await this.dropColumn(table, oldColumn)
            await this.addColumn(table, newColumn)
            clonedTable = table.clone()
        } else {
            if (newColumn.name !== oldColumn.name) {
                upQueries.push(
                    new Query(
                        `EXEC sp_rename '${this.getTablePath(table)}.${
                            oldColumn.name
                        }', '${newColumn.name}', 'COLUMN'`,
                    ),
                )
                downQueries.push(
                    new Query(
                        `EXEC sp_rename '${this.getTablePath(table)}.${
                            newColumn.name
                        }', '${oldColumn.name}', 'COLUMN'`,
                    ),
                )

                // Update FK column names that referenced the old column
                clonedTable.findColumnByName(oldColumn.name)!.name =
                    newColumn.name

                clonedTable.indices.forEach((index) => {
                    index.columnNames = index.columnNames.map((n) =>
                        n === oldColumn.name ? newColumn.name : n,
                    )
                })
                clonedTable.uniques.forEach((u) => {
                    u.columnNames = u.columnNames.map((n) =>
                        n === oldColumn.name ? newColumn.name : n,
                    )
                })
            }

            if (newColumn.isNullable !== oldColumn.isNullable) {
                upQueries.push(
                    new Query(
                        `ALTER TABLE ${this.escapePath(table)} MODIFY [${
                            newColumn.name
                        }] ${newColumn.isNullable ? "NULL" : "NOT NULL"}`,
                    ),
                )
                downQueries.push(
                    new Query(
                        `ALTER TABLE ${this.escapePath(table)} MODIFY [${
                            oldColumn.name
                        }] ${oldColumn.isNullable ? "NULL" : "NOT NULL"}`,
                    ),
                )
            }

            if (newColumn.isUnique !== oldColumn.isUnique) {
                if (newColumn.isUnique) {
                    const uniqueConstraint = new TableUnique({
                        name: this.connection.namingStrategy.uniqueConstraintName(
                            table,
                            [newColumn.name],
                        ),
                        columnNames: [newColumn.name],
                    })
                    clonedTable.uniques.push(uniqueConstraint)
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${this.escapePath(
                                table,
                            )} ADD CONSTRAINT [${
                                uniqueConstraint.name
                            }] UNIQUE ([${newColumn.name}])`,
                        ),
                    )
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${this.escapePath(
                                table,
                            )} DROP CONSTRAINT [${uniqueConstraint.name}]`,
                        ),
                    )
                } else {
                    const uniqueConstraint = clonedTable.uniques.find(
                        (u) =>
                            u.columnNames.length === 1 &&
                            u.columnNames[0] === newColumn.name,
                    )
                    if (uniqueConstraint) {
                        clonedTable.uniques.splice(
                            clonedTable.uniques.indexOf(uniqueConstraint),
                            1,
                        )
                        upQueries.push(
                            new Query(
                                `ALTER TABLE ${this.escapePath(
                                    table,
                                )} DROP CONSTRAINT [${uniqueConstraint.name}]`,
                            ),
                        )
                        downQueries.push(
                            new Query(
                                `ALTER TABLE ${this.escapePath(
                                    table,
                                )} ADD CONSTRAINT [${
                                    uniqueConstraint.name
                                }] UNIQUE ([${oldColumn.name}])`,
                            ),
                        )
                    }
                }
            }

            await this.executeQueries(upQueries, downQueries)
        }

        const foundColumn = clonedTable.findColumnByName(newColumn.name)
        if (foundColumn) {
            foundColumn.isNullable = newColumn.isNullable
            foundColumn.isUnique = newColumn.isUnique
            foundColumn.default = newColumn.default
        }
        this.replaceCachedTable(table, clonedTable)
    }

    async changeColumns(
        tableOrName: Table | string,
        changedColumns: { oldColumn: TableColumn; newColumn: TableColumn }[],
    ): Promise<void> {
        for (const { oldColumn, newColumn } of changedColumns) {
            await this.changeColumn(tableOrName, oldColumn, newColumn)
        }
    }

    async dropColumn(
        tableOrName: Table | string,
        columnOrName: TableColumn | string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const column = InstanceChecker.isTableColumn(columnOrName)
            ? columnOrName
            : table.findColumnByName(columnOrName)
        if (!column)
            throw new TypeORMError(
                `Column [${columnOrName}] was not found in the [${table.name}] table.`,
            )

        const clonedTable = table.clone()
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        // Drop related FKs, uniques, and indexes first
        clonedTable.findColumnForeignKeys(column).forEach((fk) => {
            upQueries.push(this.dropForeignKeySql(table, fk))
            downQueries.push(this.createForeignKeySql(table, fk))
            clonedTable.removeForeignKey(fk)
        })
        clonedTable.findColumnUniques(column).forEach((u) => {
            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT [${
                        u.name
                    }]`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(table)} ADD CONSTRAINT [${
                        u.name
                    }] UNIQUE ([${column.name}])`,
                ),
            )
            clonedTable.removeUniqueConstraint(u)
        })
        clonedTable.findColumnIndices(column).forEach((index) => {
            upQueries.push(this.dropIndexSql(table, index))
            downQueries.push(this.createIndexSql(table, index))
            clonedTable.removeIndex(index)
        })

        upQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(table)} DROP [${column.name}]`,
            ),
        )
        downQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    table,
                )} ADD ${this.buildCreateColumnSql(table, column)}`,
            ),
        )

        await this.executeQueries(upQueries, downQueries)
        clonedTable.removeColumn(column)
        this.replaceCachedTable(table, clonedTable)
    }

    async dropColumns(
        tableOrName: Table | string,
        columns: TableColumn[] | string[],
    ): Promise<void> {
        for (const column of columns) {
            await this.dropColumn(tableOrName, column as any)
        }
    }

    // -------------------------------------------------------------------------
    // Primary key / unique / check / exclusion DDL
    // -------------------------------------------------------------------------

    async createPrimaryKey(
        tableOrName: Table | string,
        columnNames: string[],
        constraintName?: string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const clonedTable = table.clone()

        const pkName =
            constraintName ??
            this.connection.namingStrategy.primaryKeyName(table, columnNames)
        const columnNamesSql = columnNames.map((c) => `[${c}]`).join(", ")

        const up = new Query(
            `ALTER TABLE ${this.escapePath(
                table,
            )} ADD CONSTRAINT [${pkName}] PRIMARY KEY (${columnNamesSql})`,
        )
        const down = new Query(
            `ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT [${pkName}]`,
        )

        await this.executeQueries(up, down)
        const primaryColumns = clonedTable.primaryColumns
        primaryColumns.forEach((c) => (c.isPrimary = false))
        columnNames.forEach((name) => {
            const col = clonedTable.findColumnByName(name)
            if (col) col.isPrimary = true
        })
        this.replaceCachedTable(table, clonedTable)
    }

    async updatePrimaryKeys(
        tableOrName: Table | string,
        columns: TableColumn[],
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)

        const primaryColumns = table.primaryColumns
        if (primaryColumns.length > 0) {
            const pkName =
                primaryColumns[0].primaryKeyConstraintName ??
                this.connection.namingStrategy.primaryKeyName(
                    table,
                    primaryColumns.map((c) => c.name),
                )
            await this.query(
                `ALTER TABLE ${this.escapePath(
                    table,
                )} DROP CONSTRAINT [${pkName}]`,
            )
        }

        if (columns.length > 0) {
            const pkName =
                columns[0].primaryKeyConstraintName ??
                this.connection.namingStrategy.primaryKeyName(
                    table,
                    columns.map((c) => c.name),
                )
            const columnNames = columns.map((c) => `[${c.name}]`).join(", ")
            await this.query(
                `ALTER TABLE ${this.escapePath(
                    table,
                )} ADD CONSTRAINT [${pkName}] PRIMARY KEY (${columnNames})`,
            )
        }
    }

    async dropPrimaryKey(
        tableOrName: Table | string,
        constraintName?: string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)

        const pkName =
            constraintName ??
            (table.primaryColumns[0]?.primaryKeyConstraintName ||
                this.connection.namingStrategy.primaryKeyName(
                    table,
                    table.primaryColumns.map((c) => c.name),
                ))

        const up = new Query(
            `ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT [${pkName}]`,
        )
        const down = new Query(
            `ALTER TABLE ${this.escapePath(
                table,
            )} ADD CONSTRAINT [${pkName}] PRIMARY KEY (${table.primaryColumns
                .map((c) => `[${c.name}]`)
                .join(", ")})`,
        )
        await this.executeQueries(up, down)

        const clonedTable = table.clone()
        clonedTable.primaryColumns.forEach((c) => (c.isPrimary = false))
        this.replaceCachedTable(table, clonedTable)
    }

    async createUniqueConstraint(
        tableOrName: Table | string,
        uniqueConstraint: TableUnique,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)

        uniqueConstraint.name ??=
            this.connection.namingStrategy.uniqueConstraintName(
                table,
                uniqueConstraint.columnNames,
            )

        const columnNames = uniqueConstraint.columnNames
            .map((c) => `[${c}]`)
            .join(", ")

        const up = new Query(
            `ALTER TABLE ${this.escapePath(table)} ADD CONSTRAINT [${
                uniqueConstraint.name
            }] UNIQUE (${columnNames})`,
        )
        const down = new Query(
            `ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT [${
                uniqueConstraint.name
            }]`,
        )
        await this.executeQueries(up, down)

        const clonedTable = table.clone()
        clonedTable.uniques.push(uniqueConstraint)
        this.replaceCachedTable(table, clonedTable)
    }

    async createUniqueConstraints(
        tableOrName: Table | string,
        uniqueConstraints: TableUnique[],
    ): Promise<void> {
        for (const u of uniqueConstraints)
            await this.createUniqueConstraint(tableOrName, u)
    }

    async dropUniqueConstraint(
        tableOrName: Table | string,
        uniqueOrName: TableUnique | string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const uniqueConstraint = InstanceChecker.isTableUnique(uniqueOrName)
            ? uniqueOrName
            : table.uniques.find((u) => u.name === uniqueOrName)
        if (!uniqueConstraint)
            throw new TypeORMError(
                `Supplied unique constraint was not found in table ${table.name}.`,
            )

        const up = new Query(
            `ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT [${
                uniqueConstraint.name
            }]`,
        )
        const down = new Query(
            `ALTER TABLE ${this.escapePath(table)} ADD CONSTRAINT [${
                uniqueConstraint.name
            }] UNIQUE (${uniqueConstraint.columnNames
                .map((c) => `[${c}]`)
                .join(", ")})`,
        )
        await this.executeQueries(up, down)

        const clonedTable = table.clone()
        clonedTable.removeUniqueConstraint(uniqueConstraint)
        this.replaceCachedTable(table, clonedTable)
    }

    async dropUniqueConstraints(
        tableOrName: Table | string,
        uniqueConstraints: TableUnique[],
    ): Promise<void> {
        for (const u of uniqueConstraints)
            await this.dropUniqueConstraint(tableOrName, u)
    }

    async createCheckConstraint(
        tableOrName: Table | string,
        checkConstraint: TableCheck,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)

        const up = this.createCheckConstraintSql(table, checkConstraint)
        const down = this.dropCheckConstraintSql(table, checkConstraint)
        await this.executeQueries(up, down)

        const clonedTable = table.clone()
        clonedTable.checks.push(checkConstraint)
        this.replaceCachedTable(table, clonedTable)
    }

    async createCheckConstraints(
        tableOrName: Table | string,
        checkConstraints: TableCheck[],
    ): Promise<void> {
        for (const c of checkConstraints)
            await this.createCheckConstraint(tableOrName, c)
    }

    async dropCheckConstraint(
        tableOrName: Table | string,
        checkOrName: TableCheck | string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const checkConstraint = InstanceChecker.isTableCheck(checkOrName)
            ? checkOrName
            : table.checks.find((c) => c.name === checkOrName)
        if (!checkConstraint)
            throw new TypeORMError(
                `Supplied check constraint was not found in table ${table.name}.`,
            )

        const up = this.dropCheckConstraintSql(table, checkConstraint)
        const down = this.createCheckConstraintSql(table, checkConstraint)
        await this.executeQueries(up, down)

        const clonedTable = table.clone()
        clonedTable.removeCheckConstraint(checkConstraint)
        this.replaceCachedTable(table, clonedTable)
    }

    async dropCheckConstraints(
        tableOrName: Table | string,
        checkConstraints: TableCheck[],
    ): Promise<void> {
        for (const c of checkConstraints)
            await this.dropCheckConstraint(tableOrName, c)
    }

    async createExclusionConstraint(
        _table: Table | string,
        _exclusionConstraint: TableExclusion,
    ): Promise<void> {
        throw new TypeORMError(
            "Exclusion constraints are not supported by Sybase ASE.",
        )
    }

    async createExclusionConstraints(
        _table: Table | string,
        _exclusionConstraints: TableExclusion[],
    ): Promise<void> {
        throw new TypeORMError(
            "Exclusion constraints are not supported by Sybase ASE.",
        )
    }

    async dropExclusionConstraint(
        _table: Table | string,
        _exclusionOrName: TableExclusion | string,
    ): Promise<void> {
        throw new TypeORMError(
            "Exclusion constraints are not supported by Sybase ASE.",
        )
    }

    async dropExclusionConstraints(
        _table: Table | string,
        _exclusionConstraints: TableExclusion[],
    ): Promise<void> {
        throw new TypeORMError(
            "Exclusion constraints are not supported by Sybase ASE.",
        )
    }

    // -------------------------------------------------------------------------
    // Foreign key DDL
    // -------------------------------------------------------------------------

    async createForeignKey(
        tableOrName: Table | string,
        foreignKey: TableForeignKey,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)

        foreignKey.name ??= this.connection.namingStrategy.foreignKeyName(
            table,
            foreignKey.columnNames,
            this.getTablePath(foreignKey),
            foreignKey.referencedColumnNames,
        )

        const up = this.createForeignKeySql(table, foreignKey)
        const down = this.dropForeignKeySql(table, foreignKey)
        await this.executeQueries(up, down)

        const clonedTable = table.clone()
        clonedTable.addForeignKey(foreignKey)
        this.replaceCachedTable(table, clonedTable)
    }

    async createForeignKeys(
        tableOrName: Table | string,
        foreignKeys: TableForeignKey[],
    ): Promise<void> {
        for (const fk of foreignKeys)
            await this.createForeignKey(tableOrName, fk)
    }

    async dropForeignKey(
        tableOrName: Table | string,
        foreignKeyOrName: TableForeignKey | string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const foreignKey = InstanceChecker.isTableForeignKey(foreignKeyOrName)
            ? foreignKeyOrName
            : table.foreignKeys.find((fk) => fk.name === foreignKeyOrName)
        if (!foreignKey)
            throw new TypeORMError(
                `Supplied foreign key was not found in table ${table.name}.`,
            )

        const up = this.dropForeignKeySql(table, foreignKey)
        const down = this.createForeignKeySql(table, foreignKey)
        await this.executeQueries(up, down)

        const clonedTable = table.clone()
        clonedTable.removeForeignKey(foreignKey)
        this.replaceCachedTable(table, clonedTable)
    }

    async dropForeignKeys(
        tableOrName: Table | string,
        foreignKeys: TableForeignKey[],
    ): Promise<void> {
        for (const fk of foreignKeys) await this.dropForeignKey(tableOrName, fk)
    }

    // -------------------------------------------------------------------------
    // Index DDL
    // -------------------------------------------------------------------------

    async createIndex(
        tableOrName: Table | string,
        index: TableIndex,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)

        index.name ??= this.connection.namingStrategy.indexName(
            table,
            index.columnNames,
            index.where,
        )

        const up = this.createIndexSql(table, index)
        const down = this.dropIndexSql(table, index)
        await this.executeQueries(up, down)

        const clonedTable = table.clone()
        clonedTable.indices.push(index)
        this.replaceCachedTable(table, clonedTable)
    }

    async createIndices(
        tableOrName: Table | string,
        indices: TableIndex[],
    ): Promise<void> {
        for (const index of indices) await this.createIndex(tableOrName, index)
    }

    async dropIndex(
        tableOrName: Table | string,
        indexOrName: TableIndex | string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const index = InstanceChecker.isTableIndex(indexOrName)
            ? indexOrName
            : table.indices.find((i) => i.name === indexOrName)
        if (!index)
            throw new TypeORMError(
                `Supplied index was not found in table ${table.name}.`,
            )

        const up = this.dropIndexSql(table, index)
        const down = this.createIndexSql(table, index)
        await this.executeQueries(up, down)

        const clonedTable = table.clone()
        clonedTable.removeIndex(index)
        this.replaceCachedTable(table, clonedTable)
    }

    async dropIndices(
        tableOrName: Table | string,
        indices: TableIndex[],
    ): Promise<void> {
        for (const index of indices) await this.dropIndex(tableOrName, index)
    }

    // -------------------------------------------------------------------------
    // View DDL
    // -------------------------------------------------------------------------

    async createView(view: View, syncWithMetadata = false): Promise<void> {
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        upQueries.push(this.createViewSql(view))
        if (syncWithMetadata)
            upQueries.push(await this.insertViewDefinitionSql(view))
        downQueries.push(this.dropViewSql(view))
        if (syncWithMetadata)
            downQueries.push(await this.deleteViewDefinitionSql(view))

        await this.executeQueries(upQueries, downQueries)
    }

    async dropView(
        viewOrName: View | string,
        ifExists?: boolean,
    ): Promise<void> {
        const viewName = InstanceChecker.isView(viewOrName)
            ? viewOrName.name
            : viewOrName

        if (ifExists) {
            const found = await this.loadViews([viewName])
            if (found.length === 0) return
        }

        const view = await this.getCachedView(viewName)

        const upQueries: Query[] = []
        const downQueries: Query[] = []
        upQueries.push(await this.deleteViewDefinitionSql(view))
        upQueries.push(this.dropViewSql(view))
        downQueries.push(await this.insertViewDefinitionSql(view))
        downQueries.push(this.createViewSql(view))
        await this.executeQueries(upQueries, downQueries)
    }

    // -------------------------------------------------------------------------
    // Maintenance
    // -------------------------------------------------------------------------

    async clearTable(tableOrName: Table | string): Promise<void> {
        const { tableName } = this.driver.parseTableName(tableOrName)
        await this.query(`TRUNCATE TABLE [${tableName}]`)
    }

    async clearDatabase(): Promise<void> {
        const tables: { TABLE_NAME: string }[] = await this.query(
            `SELECT name AS TABLE_NAME FROM sysobjects WHERE type = 'U' ORDER BY name`,
        )
        for (const { TABLE_NAME } of tables.reverse()) {
            await this.query(`DROP TABLE [${TABLE_NAME}]`)
        }
    }

    async changeTableComment(): Promise<void> {
        throw new TypeORMError(
            "Sybase ASE does not support table comments in this driver.",
        )
    }

    // -------------------------------------------------------------------------
    // Schema loading
    // -------------------------------------------------------------------------

    protected async loadViews(viewNames?: string[]): Promise<View[]> {
        const hasMetadataTable = await this.hasTable(
            this.getTypeormMetadataTableName(),
        )
        if (!hasMetadataTable) return []

        viewNames ??= []

        const condition = viewNames
            .map((name) => {
                const { tableName } = this.driver.parseTableName(name)
                return `"t"."name" = '${tableName}'`
            })
            .join(" OR ")

        const currentDatabase = await this.getCurrentDatabase()

        const sql =
            `SELECT "t".* FROM ${this.escapePath(
                this.getTypeormMetadataTableName(),
            )} "t" ` +
            `WHERE "t"."type" = '${MetadataTableType.VIEW}'` +
            (condition ? ` AND (${condition})` : "")

        const dbViews = await this.query(sql)
        return (dbViews as any[]).map((dbView: any) => {
            const view = new View()
            view.database = currentDatabase
            view.name = dbView["name"]
            view.expression = dbView["value"]
            return view
        })
    }

    /**
     * Loads table metadata from Sybase ASE system catalogs.
     *
     * Uses sysobjects, syscolumns, systypes, sysindexes, sysindexkeys,
     * sysforeignkeys, and sysconstraints instead of INFORMATION_SCHEMA
     * (which is incomplete in Sybase ASE 15.7).
     *
     * @param tableNames
     */
    protected async loadTables(tableNames?: string[]): Promise<Table[]> {
        if (tableNames?.length === 0) return []

        const currentDatabase = await this.getCurrentDatabase()

        // ------------------------------------------------------------------
        // 1. Resolve table list
        // ------------------------------------------------------------------
        let dbTables: {
            TABLE_NAME: string
            TABLE_CATALOG: string
            IDENTITY_COL_ID: number
        }[]

        if (!tableNames) {
            dbTables = await this.query(
                `SELECT name AS TABLE_NAME, db_name() AS TABLE_CATALOG, identitycol AS IDENTITY_COL_ID ` +
                    `FROM sysobjects WHERE type = 'U' ORDER BY name`,
            )
        } else {
            const parsed = tableNames.map((n) => {
                const { tableName, database } = this.driver.parseTableName(n)
                return { tableName, database: database ?? currentDatabase }
            })

            const condition = parsed
                .map(({ tableName }) => `name = '${tableName}'`)
                .join(" OR ")

            dbTables = await this.query(
                `SELECT name AS TABLE_NAME, db_name() AS TABLE_CATALOG, identitycol AS IDENTITY_COL_ID ` +
                    `FROM sysobjects WHERE type = 'U' AND (${condition}) ORDER BY name`,
            )
        }

        if (dbTables.length === 0) return []

        const tableNamesCondition = dbTables
            .map((t) => `o.name = '${t.TABLE_NAME}'`)
            .join(" OR ")

        // ------------------------------------------------------------------
        // 2. Columns
        // ------------------------------------------------------------------
        const dbColumns: any[] = await this.query(
            `SELECT ` +
                `o.name AS TABLE_NAME, ` +
                `c.name AS COLUMN_NAME, ` +
                `t.name AS DATA_TYPE, ` +
                `c.length AS LENGTH, ` +
                `c.prec AS NUMERIC_PRECISION, ` +
                `c.scale AS NUMERIC_SCALE, ` +
                `c.colid AS ORDINAL_POSITION, ` +
                `CASE WHEN (c.status & 8) != 0 THEN 1 ELSE 0 END AS IS_NULLABLE, ` +
                `c.cdefault AS DEFAULT_OBJ_ID, ` +
                `c.status AS STATUS ` +
                `FROM syscolumns c ` +
                `INNER JOIN sysobjects o ON o.id = c.id ` +
                `INNER JOIN systypes t ON t.usertype = c.usertype ` +
                `WHERE o.type = 'U' AND (${tableNamesCondition}) ` +
                `ORDER BY o.name, c.colid`,
        )

        // ------------------------------------------------------------------
        // 3. Primary keys via sysindexes (status & 2048 = PK flag)
        // ------------------------------------------------------------------
        const dbPrimaryKeys: any[] = await this.query(
            `SELECT ` +
                `o.name AS TABLE_NAME, ` +
                `i.name AS CONSTRAINT_NAME, ` +
                `c.name AS COLUMN_NAME, ` +
                `ik.keyno AS KEY_ORDER ` +
                `FROM sysindexes i ` +
                `INNER JOIN sysobjects o ON o.id = i.id ` +
                `INNER JOIN sysindexkeys ik ON ik.id = i.id AND ik.indid = i.indid ` +
                `INNER JOIN syscolumns c ON c.id = ik.id AND c.colid = ik.colid ` +
                `WHERE (i.status & 2048) != 0 AND o.type = 'U' ` +
                `AND (${tableNamesCondition}) ` +
                `ORDER BY o.name, ik.keyno`,
        )

        // ------------------------------------------------------------------
        // 4. Regular indexes (non-PK, non-system)
        // ------------------------------------------------------------------
        const dbIndices: any[] = await this.query(
            `SELECT ` +
                `o.name AS TABLE_NAME, ` +
                `i.name AS INDEX_NAME, ` +
                `c.name AS COLUMN_NAME, ` +
                `CASE WHEN (i.status & 2) != 0 THEN 1 ELSE 0 END AS IS_UNIQUE, ` +
                `ik.keyno AS KEY_ORDER ` +
                `FROM sysindexes i ` +
                `INNER JOIN sysobjects o ON o.id = i.id ` +
                `INNER JOIN sysindexkeys ik ON ik.id = i.id AND ik.indid = i.indid ` +
                `INNER JOIN syscolumns c ON c.id = ik.id AND c.colid = ik.colid ` +
                `WHERE o.type = 'U' AND i.indid > 0 ` +
                `AND (i.status & 2048) = 0 ` +
                `AND (i.status & 64) = 0 ` +
                `AND i.name IS NOT NULL ` +
                `AND (${tableNamesCondition}) ` +
                `ORDER BY i.name, ik.keyno`,
        )

        // ------------------------------------------------------------------
        // 5. Foreign keys via sysforeignkeys
        //    fokey1-8 / refkey1-8 are column IDs; col_name() resolves them.
        // ------------------------------------------------------------------
        const dbForeignKeys: any[] = await this.query(
            `SELECT ` +
                `object_name(fk.constrid) AS FK_NAME, ` +
                `o1.name AS TABLE_NAME, ` +
                `o2.name AS REF_TABLE_NAME, ` +
                `fk.keycnt AS KEY_COUNT, ` +
                `col_name(fk.tableid, fk.fokey1) AS FOKEY1, ` +
                `col_name(fk.tableid, fk.fokey2) AS FOKEY2, ` +
                `col_name(fk.tableid, fk.fokey3) AS FOKEY3, ` +
                `col_name(fk.tableid, fk.fokey4) AS FOKEY4, ` +
                `col_name(fk.tableid, fk.fokey5) AS FOKEY5, ` +
                `col_name(fk.tableid, fk.fokey6) AS FOKEY6, ` +
                `col_name(fk.tableid, fk.fokey7) AS FOKEY7, ` +
                `col_name(fk.tableid, fk.fokey8) AS FOKEY8, ` +
                `col_name(fk.reftabid, fk.refkey1) AS REFKEY1, ` +
                `col_name(fk.reftabid, fk.refkey2) AS REFKEY2, ` +
                `col_name(fk.reftabid, fk.refkey3) AS REFKEY3, ` +
                `col_name(fk.reftabid, fk.refkey4) AS REFKEY4, ` +
                `col_name(fk.reftabid, fk.refkey5) AS REFKEY5, ` +
                `col_name(fk.reftabid, fk.refkey6) AS REFKEY6, ` +
                `col_name(fk.reftabid, fk.refkey7) AS REFKEY7, ` +
                `col_name(fk.reftabid, fk.refkey8) AS REFKEY8 ` +
                `FROM sysforeignkeys fk ` +
                `INNER JOIN sysobjects o1 ON o1.id = fk.tableid ` +
                `INNER JOIN sysobjects o2 ON o2.id = fk.reftabid ` +
                `WHERE o1.type = 'U' AND (${tableNamesCondition.replaceAll(
                    "o.",
                    "o1.",
                )}) ` +
                `ORDER BY fk.constrid`,
        )

        // ------------------------------------------------------------------
        // 6. Check constraints via sysconstraints + syscomments
        // ------------------------------------------------------------------
        const dbCheckConstraints: any[] = await this.query(
            `SELECT ` +
                `object_name(con.constrid) AS CONSTRAINT_NAME, ` +
                `o.name AS TABLE_NAME, ` +
                `col_name(con.tableid, con.colid) AS COLUMN_NAME, ` +
                `sc.text AS CHECK_EXPRESSION ` +
                `FROM sysconstraints con ` +
                `INNER JOIN sysobjects o ON o.id = con.tableid ` +
                `INNER JOIN syscomments sc ON sc.id = con.constrid ` +
                `WHERE (con.status & 8) = 8 AND o.type = 'U' ` +
                `AND (${tableNamesCondition}) ` +
                `ORDER BY con.constrid`,
        )

        // ------------------------------------------------------------------
        // 7. Build Table objects
        // ------------------------------------------------------------------
        return dbTables.map((dbTable) => {
            const table = new Table()
            table.database = dbTable.TABLE_CATALOG
            table.name = dbTable.TABLE_NAME

            // Determine the identity column name from sysobjects.identitycol
            const identityColId = Number(dbTable.IDENTITY_COL_ID)
            const identityColumn =
                identityColId > 0
                    ? dbColumns.find(
                          (c) =>
                              c["TABLE_NAME"] === dbTable.TABLE_NAME &&
                              Number(c["ORDINAL_POSITION"]) === identityColId,
                      )
                    : null

            // ----- Columns -----
            table.columns = dbColumns
                .filter((c) => c["TABLE_NAME"] === dbTable.TABLE_NAME)
                .map((dbColumn) => {
                    const tableColumn = new TableColumn()
                    tableColumn.name = dbColumn["COLUMN_NAME"]
                    tableColumn.type = dbColumn["DATA_TYPE"].toLowerCase()
                    tableColumn.isNullable =
                        Number(dbColumn["IS_NULLABLE"]) === 1

                    tableColumn.isGenerated =
                        identityColumn?.["COLUMN_NAME"] ===
                        dbColumn["COLUMN_NAME"]
                    if (tableColumn.isGenerated)
                        tableColumn.generationStrategy = "increment"

                    // Length for character/binary types
                    if (
                        this.driver.withLengthColumnTypes.indexOf(
                            tableColumn.type as ColumnType,
                        ) !== -1 &&
                        dbColumn["LENGTH"]
                    ) {
                        const len = String(dbColumn["LENGTH"])
                        if (
                            !this.driver.isDefaultColumnLength(
                                table,
                                tableColumn,
                                len,
                            )
                        ) {
                            tableColumn.length = len
                        }
                    }

                    // Precision / scale for numeric types
                    if (
                        this.driver.withPrecisionColumnTypes.indexOf(
                            tableColumn.type as ColumnType,
                        ) !== -1
                    ) {
                        if (dbColumn["NUMERIC_PRECISION"] !== null) {
                            const precision = Number(
                                dbColumn["NUMERIC_PRECISION"],
                            )
                            if (
                                !this.driver.isDefaultColumnPrecision(
                                    table,
                                    tableColumn,
                                    precision,
                                )
                            )
                                tableColumn.precision = precision
                        }
                        if (dbColumn["NUMERIC_SCALE"] !== null) {
                            const scale = Number(dbColumn["NUMERIC_SCALE"])
                            if (
                                !this.driver.isDefaultColumnScale(
                                    table,
                                    tableColumn,
                                    scale,
                                )
                            )
                                tableColumn.scale = scale
                        }
                    }

                    // isPrimary
                    tableColumn.isPrimary = dbPrimaryKeys.some(
                        (pk) =>
                            pk["TABLE_NAME"] === dbTable.TABLE_NAME &&
                            pk["COLUMN_NAME"] === dbColumn["COLUMN_NAME"],
                    )

                    // isUnique – single-column unique index
                    const singleColUnique = dbIndices.find(
                        (idx) =>
                            idx["TABLE_NAME"] === dbTable.TABLE_NAME &&
                            idx["COLUMN_NAME"] === dbColumn["COLUMN_NAME"] &&
                            Number(idx["IS_UNIQUE"]) === 1 &&
                            !dbIndices.some(
                                (other) =>
                                    other["INDEX_NAME"] === idx["INDEX_NAME"] &&
                                    other["COLUMN_NAME"] !==
                                        dbColumn["COLUMN_NAME"],
                            ),
                    )
                    tableColumn.isUnique = !!singleColUnique

                    return tableColumn
                })

            // ----- Primary key constraint -----
            const pkColumns = dbPrimaryKeys
                .filter((pk) => pk["TABLE_NAME"] === dbTable.TABLE_NAME)
                .sort((a, b) => Number(a["KEY_ORDER"]) - Number(b["KEY_ORDER"]))
            if (pkColumns.length > 0) {
                table.columns.forEach((col) => {
                    if (
                        pkColumns.some((pk) => pk["COLUMN_NAME"] === col.name)
                    ) {
                        col.isPrimary = true
                        col.primaryKeyConstraintName =
                            pkColumns[0]["CONSTRAINT_NAME"]
                    }
                })
            }

            // ----- Indices -----
            const indexNames = OrmUtils.uniq(
                dbIndices
                    .filter((i) => i["TABLE_NAME"] === dbTable.TABLE_NAME)
                    .map((i) => i["INDEX_NAME"]),
            )

            table.indices = indexNames.map((indexName) => {
                const indexRows = dbIndices
                    .filter(
                        (i) =>
                            i["TABLE_NAME"] === dbTable.TABLE_NAME &&
                            i["INDEX_NAME"] === indexName,
                    )
                    .sort(
                        (a, b) =>
                            Number(a["KEY_ORDER"]) - Number(b["KEY_ORDER"]),
                    )
                return new TableIndex({
                    name: indexName,
                    columnNames: indexRows.map((r) => r["COLUMN_NAME"]),
                    isUnique: Number(indexRows[0]["IS_UNIQUE"]) === 1,
                })
            })

            // ----- Unique constraints (single-column unique indexes) -----
            table.uniques = table.indices
                .filter(
                    (idx) =>
                        idx.isUnique &&
                        idx.columnNames.length === 1 &&
                        !pkColumns.some(
                            (pk) => pk["COLUMN_NAME"] === idx.columnNames[0],
                        ),
                )
                .map(
                    (idx) =>
                        new TableUnique({
                            name: idx.name,
                            columnNames: idx.columnNames,
                        }),
                )

            // ----- Foreign keys -----
            table.foreignKeys = dbForeignKeys
                .filter((fk) => fk["TABLE_NAME"] === dbTable.TABLE_NAME)
                .map((fkRow) => {
                    const keyCnt = Math.min(Number(fkRow["KEY_COUNT"]), 8)
                    const columnNames: string[] = []
                    const referencedColumnNames: string[] = []
                    for (let i = 1; i <= keyCnt; i++) {
                        const col = fkRow[`FOKEY${i}`]
                        const ref = fkRow[`REFKEY${i}`]
                        if (col) columnNames.push(col)
                        if (ref) referencedColumnNames.push(ref)
                    }
                    return new TableForeignKey({
                        name: fkRow["FK_NAME"],
                        referencedTableName: fkRow["REF_TABLE_NAME"],
                        columnNames,
                        referencedColumnNames,
                    })
                })

            // ----- Check constraints -----
            table.checks = dbCheckConstraints
                .filter((c) => c["TABLE_NAME"] === dbTable.TABLE_NAME)
                .map(
                    (c) =>
                        new TableCheck({
                            name: c["CONSTRAINT_NAME"],
                            columnNames: c["COLUMN_NAME"]
                                ? [c["COLUMN_NAME"]]
                                : [],
                            expression: c["CHECK_EXPRESSION"],
                        }),
                )

            return table
        })
    }

    // -------------------------------------------------------------------------
    // Protected helpers
    // -------------------------------------------------------------------------

    protected createViewSql(view: View): Query {
        const expression =
            typeof view.expression === "string"
                ? view.expression
                : view.expression(this.connection).getQuery()
        return new Query(
            `CREATE VIEW ${this.escapePath(view)} AS ${expression}`,
        )
    }

    protected dropViewSql(viewOrPath: View | string): Query {
        return new Query(`DROP VIEW ${this.escapePath(viewOrPath)}`)
    }

    protected async insertViewDefinitionSql(view: View): Promise<Query> {
        const { tableName: name } = this.driver.parseTableName(view)
        const expression =
            typeof view.expression === "string"
                ? view.expression.trim()
                : view.expression(this.connection).getQuery()
        return this.insertTypeormMetadataSql({
            database: view.database,
            type: MetadataTableType.VIEW,
            name,
            value: expression,
        })
    }

    protected async deleteViewDefinitionSql(
        viewOrPath: View | string,
    ): Promise<Query> {
        const { database, tableName: name } =
            this.driver.parseTableName(viewOrPath)
        return this.deleteTypeormMetadataSql({
            database,
            type: MetadataTableType.VIEW,
            name,
        })
    }

    /**
     * Returns the escaped, fully-qualified table path for use in SQL statements.
     * Sybase uses "database".."tableName" for cross-database access.
     *
     * @param target
     */
    protected escapePath(target: Table | View | string): string {
        const { database, tableName } = this.driver.parseTableName(target)
        if (database && database !== this.driver.database) {
            return `[${database}]..[${tableName}]`
        }
        return `[${tableName}]`
    }

    /**
     * Generates CREATE TABLE SQL for the given table.
     *
     * @param table
     * @param createForeignKeys
     */
    protected createTableSql(table: Table, createForeignKeys?: boolean): Query {
        const columnDefinitions = table.columns
            .map((col) => this.buildCreateColumnSql(table, col))
            .join(", ")
        let sql = `CREATE TABLE ${this.escapePath(table)} (${columnDefinitions}`

        // Named UNIQUE constraints
        if (table.uniques.length > 0) {
            const uniquesSql = table.uniques
                .map((u) => {
                    const name =
                        u.name ??
                        this.connection.namingStrategy.uniqueConstraintName(
                            table,
                            u.columnNames,
                        )
                    return `CONSTRAINT [${name}] UNIQUE (${u.columnNames
                        .map((c) => `[${c}]`)
                        .join(", ")})`
                })
                .join(", ")
            sql += `, ${uniquesSql}`
        }

        // Named CHECK constraints
        if (table.checks.length > 0) {
            const checksSql = table.checks
                .map((c) => {
                    const name =
                        c.name ??
                        this.connection.namingStrategy.checkConstraintName(
                            table,
                            c.expression!,
                        )
                    return `CONSTRAINT [${name}] CHECK (${c.expression})`
                })
                .join(", ")
            sql += `, ${checksSql}`
        }

        // Inline FOREIGN KEY constraints
        if (createForeignKeys && table.foreignKeys.length > 0) {
            const fksSql = table.foreignKeys
                .map((fk) => {
                    const cols = fk.columnNames.map((c) => `[${c}]`).join(", ")
                    fk.name ??= this.connection.namingStrategy.foreignKeyName(
                        table,
                        fk.columnNames,
                        this.getTablePath(fk),
                        fk.referencedColumnNames,
                    )
                    const refCols = fk.referencedColumnNames
                        .map((c) => `[${c}]`)
                        .join(", ")
                    let constraint =
                        `CONSTRAINT [${fk.name}] FOREIGN KEY (${cols}) ` +
                        `REFERENCES ${this.escapePath(
                            this.getTablePath(fk),
                        )} (${refCols})`
                    if (fk.onDelete) constraint += ` ON DELETE ${fk.onDelete}`
                    if (fk.onUpdate) constraint += ` ON UPDATE ${fk.onUpdate}`
                    return constraint
                })
                .join(", ")
            sql += `, ${fksSql}`
        }

        // PRIMARY KEY constraint
        const primaryColumns = table.columns.filter((c) => c.isPrimary)
        if (primaryColumns.length > 0) {
            const pkName =
                primaryColumns[0].primaryKeyConstraintName ??
                this.connection.namingStrategy.primaryKeyName(
                    table,
                    primaryColumns.map((c) => c.name),
                )
            const cols = primaryColumns.map((c) => `[${c.name}]`).join(", ")
            sql += `, CONSTRAINT [${pkName}] PRIMARY KEY (${cols})`
        }

        sql += `)`
        return new Query(sql)
    }

    protected dropTableSql(
        tableOrName: Table | string,
        ifExists?: boolean,
    ): Query {
        const sql = ifExists
            ? `IF OBJECT_ID('${
                  this.driver.parseTableName(tableOrName).tableName
              }') IS NOT NULL ` + `DROP TABLE ${this.escapePath(tableOrName)}`
            : `DROP TABLE ${this.escapePath(tableOrName)}`
        return new Query(sql)
    }

    protected createIndexSql(table: Table, index: TableIndex): Query {
        const cols = index.columnNames.map((c) => `[${c}]`).join(", ")
        const unique = index.isUnique ? "UNIQUE " : ""
        return new Query(
            `CREATE ${unique}INDEX [${index.name}] ON ${this.escapePath(
                table,
            )} (${cols})`,
        )
    }

    protected dropIndexSql(
        table: Table,
        indexOrName: TableIndex | string,
    ): Query {
        const indexName = InstanceChecker.isTableIndex(indexOrName)
            ? indexOrName.name!
            : indexOrName
        return new Query(
            `DROP INDEX [${indexName}] ON ${this.escapePath(table)}`,
        )
    }

    protected createForeignKeySql(
        table: Table,
        foreignKey: TableForeignKey,
    ): Query {
        const cols = foreignKey.columnNames.map((c) => `[${c}]`).join(", ")
        const refCols = foreignKey.referencedColumnNames
            .map((c) => `[${c}]`)
            .join(", ")
        let sql =
            `ALTER TABLE ${this.escapePath(table)} ADD CONSTRAINT [${
                foreignKey.name
            }] ` +
            `FOREIGN KEY (${cols}) REFERENCES ${this.escapePath(
                this.getTablePath(foreignKey),
            )} (${refCols})`
        if (foreignKey.onDelete) sql += ` ON DELETE ${foreignKey.onDelete}`
        if (foreignKey.onUpdate) sql += ` ON UPDATE ${foreignKey.onUpdate}`
        return new Query(sql)
    }

    protected dropForeignKeySql(
        table: Table,
        foreignKeyOrName: TableForeignKey | string,
    ): Query {
        const fkName = InstanceChecker.isTableForeignKey(foreignKeyOrName)
            ? foreignKeyOrName.name!
            : foreignKeyOrName
        return new Query(
            `ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT [${fkName}]`,
        )
    }

    protected createCheckConstraintSql(table: Table, check: TableCheck): Query {
        return new Query(
            `ALTER TABLE ${this.escapePath(table)} ADD CONSTRAINT [${
                check.name
            }] CHECK (${check.expression})`,
        )
    }

    protected dropCheckConstraintSql(
        table: Table,
        checkOrName: TableCheck | string,
    ): Query {
        const name = InstanceChecker.isTableCheck(checkOrName)
            ? checkOrName.name!
            : checkOrName
        return new Query(
            `ALTER TABLE ${this.escapePath(table)} DROP CONSTRAINT [${name}]`,
        )
    }

    /**
     * Builds the column definition SQL fragment used in CREATE TABLE and ALTER TABLE ADD.
     *
     * @param _table
     * @param column
     * @param skipIdentity
     */
    protected buildCreateColumnSql(
        _table: Table,
        column: TableColumn,
        skipIdentity = false,
    ): string {
        let c = `[${column.name}] ${this.driver.createFullType(column)}`

        if (column.collation) c += ` COLLATE ${column.collation}`

        if (!column.isNullable) c += " NOT NULL"

        if (
            column.isGenerated &&
            column.generationStrategy === "increment" &&
            !skipIdentity
        ) {
            c += " IDENTITY"
        }

        if (column.default !== undefined && column.default !== null) {
            c += ` DEFAULT ${column.default}`
        }

        return c
    }
}
