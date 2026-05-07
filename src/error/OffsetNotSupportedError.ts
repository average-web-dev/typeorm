import { TypeORMError } from "./TypeORMError"

/**
 * Thrown when user tries to build a query using OFFSET but the database does not support it.
 */
export class OffsetNotSupportedError extends TypeORMError {
    constructor() {
        super(`Sybase does not support OFFSET in statements.`)
    }
}
