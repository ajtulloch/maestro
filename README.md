maestro
=======

```
maestro: a distinguished conductor
```

The `maestro` library is responsible for providing convenient APIs for marshalling and
orchestrating data around for etl type work.

The primary goal of `maestro` is to provide the ability to make it _easy_ to manage
data sets with out sacrificing safety or robustness. This is achieved by sticking
with strongly-typed schemas describing the fixed structure of data, and working on
APIs for manipulating those structures in a sensible way that it can scale to data-sets
with 100s of columns.


starting point
--------------

`maestro` is designed to work with highly structured data. It is
expected that all data-sets manipulated by `maestro` at some level
(maybe input, output or intermediate representations) have a well
defined wide row schema and fixed set of columns.

At this point, `maestro` supports `thrift` for schema definitions.

`maestro` uses the thrift schema definition to derive as much meta-data and
implementation of custom processing (such as printing and parsing) as it
can. It hen provides APIs that use these "data type" specific tools to
provide generic "tasks" like generating analytics views.


5 minute quick-start
--------------------

### Defining a thrift schema.

This is not the place for a full thrift tutorial, so I will skip a lot
of the details, but if you don't understand thrift or would like more
complete documentation then <http://diwakergupta.github.io/thrift-missing-guide/>
is a really good reference.

So if a dataset was going to land on the system, we would define a
schema accurately defining the columns and types:

```

#@namespace scala au.com.cba.omnia.etl.customer.thrift

struct Customer {
  1  : string CUSTOMER_ID
  2  : string CUSTOMER_NAME
  3  : string CUSTOMER_ACCT
  4  : string CUSTOMER_CAT
  5  : string CUSTOMER_SUB_CAT
  6  : i32 CUSTOMER_BALANCE
  7  : string EFFECTIVE_DATE
 }

```

This is a simplified example, a real data set may have 100s of
columns, but it should be enough to demonstrate. The important points
here are that _order_ is important, the struct should be defined to
have fields in the same order as input data, and _types_ are
important, they should accurately descript the data (and will be used
to infer how the data should be parsed and validated).


### Building a pipeline from the built in tools

A pipeline is defined in terms of a `cascade` this terminology comes
from the underlying technology used, but it is easier to think of it
as a set of jobs (with an implied partial-ordering, based on data
dependencies).

A cascade can be made up of scalding jobs, hive queries (future),
shell commands, or `maestro` tasks. The hope is that most pipelines
can be implemented with _just_ `maestro`, and the corner cases can
be easily handled by hive or raw scalding jobs.

A pipeline built only from `maestro` tasks.

```scala

import com.twitter.scalding._
import au.com.cba.omnia.maestro._
import au.com.cba.omnia.etl.customer.thrift._

class CustomerCuscade(args: Args) extends CascadeJob(args) with MaestroSupport[Customer] {
  val maestro = Maestro(args)

  val delimiter = "|$|"
  val env           = args("env")
  val domain        = "CUSTOMER"
  val input         = s"${env}/source/${domain}"
  val clean         = s"${env}/processing/${domain}"
  val outbound      = s"${env}/outbound/${domain}"
  val dateView      = s"${env}/view/warehouse/${domain}/by-date"
  val catView       = s"${env}/view/warehouse/${domain}/by-category"
  val features      = s"${env}/view/features/ivory"
  val errors        = s"${env}/errors/${domain}"
  val now           = yyyyMMdd.format(new java.util.Date)
  val byDate        = Partition.byDate(Fields.EFFECITVE_DATE)
  val byCategory    = Partition.byFields(Field.CUSTOMER_CAT, Fields.CUSTOMER_SUB_CAT)
  val filters       = Filter.exclude(Fields.EFFECTIVE_DATE)
  val cleaners      = Clean.all(
    Clean.trim
  )
  val validators    = Validator.all(
  )
  val filter        = RowFilter.keep

  def jobs = List(
    maestro.load[Customer](delimiter, input, clean, errors, now, cleaners, validators, filter),
    maestro.view(byDate, clean, dateView),
    maestro.view(byCategory, clean, catView),
    maestro.ivory(clean, features),
    maestro.sqoop(clean, output, filters)
  )
}

```

### Concepts

#### MaestroSupport

`MaestroSupport` uses the metadata available from the thrift definition
to generate out significant supporting infrastructure customized for
_your_ specific record type. This gives us the ability to refer to fields
for partitioning, filtering and validation in a way that can be easily
be checked and validated up front (by the compiler in most circumstances,
and on start-up before things run in the worst case) and that those field
references can have large amounts of interesting metadata which allows
us to automatically parse, print, validate, filter, partition the data
in a way that we _know_ will work before we run the code (for a valid
schema).

#### Tasks

Tasks are not a concrete concept, it is just a name used to indicate that a
function returns a scalding job for integration into a cascade. Tasks differ
from raw Jobs in that they rely on the metadata generated by `MaestroSupport`
and can generally only by executed via their scala api and are not intended to
be standalone jobs that would be run by themselves.

#### Partitioners

Partitioners are really simple. Partitioners are just a list of fields to
partition a data set by.

The primary api is the list of fields you want to partition on:

```scala
Partiton.byFields(Fields.CUSTOMER_CAT, Fields.CUSTOMER_SUB_CAT)
```

The api also has special support for dates of the `yyyy-MM-dd` form:

```scala
Partiton.byDate(Fields.EFFECTIVE_DATE)
```

This will use that field, but split the partitioning into 3 parts of
yyyy, MM and dd.


#### Filters

Filters gain are really simple (hopefully). By default filters
allow everything. A filter can then be refined via either
blacklists (i.e. exclude these columns) or whitelists (i.e.
only include these columns).

Examples:
```scala
 // everything except effective date
Filter.exclude(Fields.EFFECTIVE_DATE)

 // _only_ effective date and customer id
Filter.include(Fields.EFFECTIVE_DATE, Fields.CUSTOMER_ID)
```

#### Validators

Validators start to get a bit more advanced, but hopefully not too bad.
A Validator can be thought of something that is a function from the record
type to either an error message or an "ok" tick of approval. In a lot of
cases this understanding can be simplified to saying it is a combination
of a `Field` to validate and a `Check` to apply. There are a few builtin
checks provided, if you want to do custom checking you can fail back to
defining a custom function.

```scala
Validator.all(
  Validator.of(fields.EFFECTIVE_DATE, Check.isDate),
  Validator.of(fields.CUSTOMER_CAT, Check.oneOf("BANK", "INSURE")),
  Validator.of(fields.CUSTOMER_NAME, Check.nonempty),
  Validator.of(fields.CUSTOMER_ID, Check.matches("\d+")),
  Validator.by[Customer](_.customerAcct.length == 4, "Customer accounts should always be a length of 4")
)
```

### Advanced tips & tricks

The best tip for advanced pipelines is to look carefully at how
the maestro tasks are implemented. You have the same set of tools
available to you and can jump down to the same lower-level of
abstraction by just implementing a scalding job, and extending the
MaestroSupport trait.

### Type Mappings

| Thrift Type                                       | Hive Type                                                                                     | Scala Type    |
| ------------------------------------------------- |:---------------------------------------------------------------------------------------------:| -------------:|
| bool: A boolean value (true or false), one byte   | BOOLEAN                                                                                       | bool          |
| byte: A signed byte                               | TINYINT (1-byte signed integer, from -128 to 127)                                             | byte          |
| i16: A 16-bit signed integer                      | SMALLINT (2-byte signed integer, from -32,768 to 32,767)                                      | short         |
| i32: A 32-bit signed integer                      | INT (4-byte signed integer, from -2,147,483,648 to 2,147,483,647)                             | int           |
| i64: A 64-bit signed integer                      | BIGINT (8-byte signed integer, from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)  | BigInteger    |
| double: A 64-bit floating point number            | DOUBLE (8-byte double precision floating point number)                                        | double        |
| string: Encoding agnostic text or binary string   | string                                                                                        | string        |

