import type { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions"
import type { SybaseConnectionCredentialsOptions } from "./SybaseConnectionCredentialsOptions"

/**
 * Sybase ASE specific connection options.
 */
export interface SybaseConnectionOptions
    extends BaseDataSourceOptions,
        SybaseConnectionCredentialsOptions {
    /**
     * Database type.
     */
    readonly type: "sybase"

    /**
     * Character set to use for the connection (e.g. 'utf8', 'iso_1').
     */
    readonly charset?: string

    /**
     * Language to use for the connection (e.g. 'us_english').
     */
    readonly language?: string

    /**
     * TDS packet size in bytes (must be a power of 2). Default: 512.
     */
    readonly packetSize?: number

    /**
     * Connection pool configuration.
     */
    readonly pool?: {
        /**
         * Minimum number of connections to keep in the pool. Default: 0.
         */
        readonly min?: number

        /**
         * Maximum number of connections to create. 0 means unlimited. Default: 10.
         */
        readonly max?: number

        /**
         * Milliseconds before an idle connection is closed. Default: 60000.
         */
        readonly idleTimeout?: number
    }

    /**
     * Override the underlying sybase-tds driver module (useful for testing).
     */
    readonly driver?: any

    readonly poolSize?: never
}
