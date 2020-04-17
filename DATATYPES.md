cstar_fdw datatype support
=============

## Numeric Datatypes

### Read Support
smallint
int
bigint
float
double
counter

### Write Support
smallint
int
bigint
float
double

Note: Counter is mapped to the PostgreSQL bigint datatype.

## Text Types

### Read Support
text
ascii
varchar

### Write Support
text
ascii
varchar

## Date Time Types

### Read Support
timestamp

### Write Support
timestamp

Note: If the time zone is not specified while writing into a timestamp column the timezone of the PostgreSQL DB server is used.

## Other Datatypes

### Read/Write Support
boolean
inet
uuid