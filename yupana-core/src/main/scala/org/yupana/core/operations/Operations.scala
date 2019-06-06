package org.yupana.core.operations

trait Operations extends UnaryOperationsImpl with BinaryOperationsImpl with WindowOperationsImpl with AggregationsImpl

object Operations extends Operations with Serializable
