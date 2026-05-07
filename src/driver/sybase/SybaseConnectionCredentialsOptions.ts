/**
 * Sybase ASE specific connection credential options.
 */
export interface SybaseConnectionCredentialsOptions {
    /**
     * Database host.
     */
    readonly host?: string

    /**
     * Database host port. Default: 5000.
     */
    readonly port?: number

    /**
     * Database name to connect to.
     */
    readonly database?: string

    /**
     * Database username.
     */
    readonly username?: string

    /**
     * Database password.
     */
    readonly password?: string
}
