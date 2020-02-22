package com.github.yuizho.dbraccoon.exception

internal class DbRaccoonDataSetException : RuntimeException {
    constructor() : super()
    constructor(message: String, cause: Throwable) : super(message, cause)
    constructor(message: String) : super(message)
    constructor(cause: Throwable) : super(cause)
}