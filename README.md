# pgsql-tools
Various small tools for PostgreSQL DBAs

## UIBK branch

### find-iit-connections.pl

Analyze query-level logfiles and group them together by session and/or transaction. Within each query group,
for each statement, report its duration and start-time, relative to the start of the query-group.

#### Requisites:

Postgresql configuration must contain:
 * `log_min_duration_statement = 0`
 * `log_line_prefix` must incude `%m` and should include `%c` (instead of `%p`). I recommend `%m\t%d\t%u\t%c\t%l\t%e\t`

It's highly recommended that:
 * Logs are saved to a tmpfs mount or ramdisk
 * Size-based rotation is enabled
 * An external log capturer or rotator is used to ensure the disk doesn't get full. I use `runit`'s `svlogd` utility.
 
#### Usage:  <program> [options] [logfile ... ]

Where `options` are one of 
* `-p`  Specify string used with `log_line_prefix`
* `-d`  Comma-list of database names to analyze (excluding others). Replaces command-line parameter from main branch. The `*` is no longer magical, and a blank or empty string means "all".
* `-t`  Specifies the (minimum) threshold for reporting of the *t*ime in a transaction.
* `-x`  Report only on transactions (*x*actions), ie, `BEGIN ... COMMIT` or ending in `ROLLBACK` or disconnection.
* `-m`  Report only on sessions/transactions with DML (insert/update/delete)

The program reads from `stdin` and then from any files on the command-line (perl's `<>` operator). 
Errors, as well as a progress-meter are output to `stderr`. 

#### Example usage and output:

```
$ perl   ~/pgsql-tools/find-iit-connections.pl -m -x -t 1000 -d xample pg-Monday.log
 
 2016-01-28 17:18:05.391 CET [56] "xample" total duration: 1010.15 ms
 +     0ms   0.01ms: select 1
 +   271ms   0.01ms: BEGIN
 +   271ms   0.09ms: select persistedm0_.id as id1_69_, persistedm0_.creationdate as creation2_69_, persistedm0_.expirationdate as expirati3_69_, persistedm0_.lastmodified as lastmodi4_69_, persistedm0_.mapper_uuid as mapper_u5_69_, persistedm0_.orig_session_id as orig_ses6_69_, persistedm0_.xml_config as xml_conf7_69_ from o_mapper persistedm0_ where persistedm0_.mapper_uuid=$1 order by persistedm0_.id limit $2
 +  1543ms 1003.27ms: update o_mapper set expirationdate=$1, lastmodified=$2, xml_config=$3 where id=$4
 +  1694ms   6.76ms: COMMIT
```

#### BUGS

* Currently, the time offset shows when the statement completed, not started. 
The original (master) branch adjusted the duration from the completion time, and I had thought that step was an error. 
* We cannot distinguish between `END` of a transaction and `END` of a procedural or `CASE` block. So we ignore `END` altogether.

#### TODO

* Grab log_line_prefix from existing PG installation, if available; override with option
* Find "hidden" DML statements
* Allow user to specify list of functions/procedures that write to db
* Actually determine idle time
   * ? Should it be the total idle time?
   * ? OR max idle time between statements?

