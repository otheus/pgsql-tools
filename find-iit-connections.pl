#!/usr/bin/perl
use strict;

use File::Spec;
use Data::Dumper;
use POSIX qw(strftime);
use Fatal qw(open);
use bignum;
use Getopt::Std;

our $COMMIT_SQL = qr{\b(?:COMMIT|ROLLBACK|ABORT)\s*;?\s*}i; # END removed.
our $BEGIN_SQL  = qr{\b(?:BEGIN|START)\b}i;
our $DML_SQL  = qr{\b(?:INSERT|UPDATE|DELETE)\b}i; # imperfect, but better than nothing

our $opt_p = '%m\t%d\t%u\t%c\t%l\t%e\t'; # Prefix on each non-continuation log line
our $opt_d = ''; # DB to analyze
our $opt_t = 0;  # Max time in a transaction
our $opt_T = 0;  # Max time between statements within a transaction
 # ^ (not yet implemented)
our $opt_x = 0;  # Log Xactions only?
our $opt_m = 0;  # modifications (insert/update/delete) only?
getopts('mxp:d:t:');

my ( $PREFIX, @PREFIX_ELEMENTS ) = get_optional_header_re( $opt_p ) ;
die "Log prefix must contain at least a timestamp (%m), and session id (%c) or process id (%p)"
  unless grep(/^m$/,@PREFIX_ELEMENTS) && grep (/^[cp]$/,@PREFIX_ELEMENTS);
my $MINIMAL_IDLE_TO_REPORT = $opt_t;
my @ANALYZE_DB = split(',\s*',$opt_d);

our %sessions = ();
exit !parse_input();

#------------------------------------------------------------------------------------------------

sub get_optional_header_re {
    my $prefix = shift;

    my %re = (
        'u' => '(?:\[unknown\]|[[:alnum:]_]*)', # empty, [unknown], or username
        'd' => '(?:\[unknown\]|[[:alnum:]_]*)', # empty, [unknown], or dbname
        'r' => '\d{1,3}(?:\.\d{1,3}){3}\(\d+\)|\[local\]',
        'h' => '\d{1,3}(?:\.\d{1,3}){3}|\[local\]',
        'p' => '\d+',
        't' => '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d \S+',
        'm' => '\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+ \S+',
        'l' => '\d+',
        'i' => '(?:BEGIN|COMMIT|DELETE|INSERT|ROLLBACK|SELECT|SET|SHOW|UPDATE)',
	'e' => '\w{5}',
	'c' => '[[:alnum:]]+\.[[:alnum:]]+',
    );

    my @known_keys = keys %re;
    my $known_re = join '|', @known_keys;

    my @matched = ();

    $prefix =~ s/([()\[\]])/\\$1/g;
    $prefix =~ s/%($known_re)/push @matched, $1;'('.$re{$1}.')'/ge;
    return qr/^$prefix/, @matched;
}

sub postmatch { 
  # See http://www.perlmonks.org/?node_id=291363
  substr($_[0],$+[0]); # same as $POSTMATCH, but faster
}
sub match {
  substr( $_[0], $-[0], $+[0] - $-[0] )
}
sub prematch {
  substr( $_[0], 0, $-[0] )
}

sub parse_input {
    my $valid_input=0;
    my $previous = {};

    while ( my $line = <> ) {
        print STDERR '#' if 0 == $. % 1000;
        chomp $line;
	my $statement="";

        my @temp;
        my %prefix;
        if ( scalar( @temp = $line =~ $PREFIX ) ) {
	  $statement = postmatch($line); 
	  @prefix{ @PREFIX_ELEMENTS } = @temp;
	  $prefix{'_'}=($prefix{'c'}||$prefix{'p'});
	} else {
	    # that's ok. continuation line.
	    if ( length $previous->{ 'sql' } ) { 
	      $previous->{ 'sql' } .= ' ' . $line;
	    }
            elsif (length $previous->{ 'm' } ) { 
	      # ignore - extended from prevous line ? DETAIL? ERROR? 
	    }
	    else { 
	      warn "Unrecognized log line format";
	    }
	    next;
	}

        if ( $statement =~ m{ \A LOG: \s+ duration: \s+ (\d+\.\d+) \s+ ms \s+ ( statement | execute [^:]* ): \s+ (.*?) \s* \z }xms ) {
            my ( $duration, $phase, $sql ) = ( $1, $2, $3 );
	    # Future: phase could be "parse" , "bind"
	    process_query( $previous ) if $previous->{ 'sql' } ;
	    $previous = {
		  'duration'  => $duration,
		  'sql'       => $sql,
		  %prefix,
	    };
	    next;
        }

        if ( $statement =~ m{ \A (?: LOG | STATEMENT | NOTICE | HINT | DETAIL | FATAL | WARNING | PANIC | ERROR ) : \s{1,2} }xms ) {
            process_query( $previous ) if $previous->{ 'sql' };
            $previous = { %prefix };
        }
	elsif ($statement =~ m{ \A FATAL: \s+ the database system is shutting down \\}xms ) {
	    process_query( $previous ) if $previous->{ 'sql' } ;
	    $previous = {};
	    flush_all_sessions();
	}
	elsif ( $statement =~ m{ \A LOG: \s+ disconnection: session time: (\d+:\d+:\d+\.\d+) }xms ) {
	    process_query( $previous ) if $previous->{ 'sql' } ;
	    $previous = {};
	    flush_session( $previous->{'_'}, 1);
	}
        else {
	    warn "Unrecognized statement format: $statement";
        }
    }
    process_query( $previous ) if $previous->{ 'sql' };
    flush_all_sessions();
    return $valid_input;
}

sub reset_session { 
  my $rh_session = shift;
  my $timestamp = shift;
  $rh_session->{ 'time' } = $timestamp; 
  $rh_session->{ 'duration' } = 0.0;
  $rh_session->{ 'maxidletime' } = 0.0;
  $rh_session->{ 'xact' } = 0;
  $rh_session->{ 'dml' } = 0;
  $rh_session->{ 'queries' }=[];
}

sub flush_session {
  my $sid = shift;
  my $deleteit = shift;
  if (exists $sessions{ $sid } ) { 
    my $session = $sessions{ $sid };
    my ($pid) = $sid =~ /(\d+)$/;
    my $basetime = get_ms_from_timestamp( $session->{ 'time' } );
    my $lasttime = $basetime;

    if (scalar(@{$session->{ 'queries' }})) {
      if (
	  (!$opt_m || $session->{ 'dml' }) and
	  (!$opt_x || $session->{ 'xact' }) and
	  (!$opt_t || $session->{ 'duration' } > $opt_t)
      ) { 
	printf( "%s [%d] \"%s\" total duration: %.2f ms\n",  $session->{ 'time' }, $sid, ($session->{ 'dbname' } || "-"), $session->{ 'duration' } );
	foreach (@{$session->{ 'queries' }}) { 
	  my ($querytime,$duration,$sql)= @{$_};
	  my $starttime = get_ms_from_timestamp( $querytime );
	  #printf(" +%6dms %5.1fms: %s\n",($starttime - $basetime),int($duration * 1000),$sql);
	  printf(" +%6dms %6.2fms: %s\n",($starttime - $basetime),$duration,$sql);
	  $lasttime = $querytime;
	}
	print "\n";
      }
      reset_session( $session, $lasttime );
    }
    delete $session->{ $sid } if $deleteit;
  }
}

#    my $previous_end = $sessions{ $sid }->{ 'last' };
#    if ( $beginning_time - $previous_end >= $MINIMAL_IDLE_TO_REPORT ) {
#    printf( "%ums @ %s (pid: %s), queries:\n", $beginning_time - $previous_end, $d->{ 'prefix' }->{ 'm' }, $sid );
#    print "  - $_\n" for ( @{ $sessions{ $sid }->{ 'queries' } }, $d->{ 'sql' } );
#    print "\n";
#    }
sub flush_all_sessions { 
  foreach (keys %sessions) { 
    flush_session($_,1);
  }
}

#
sub add_query {
  my $rh_session = shift;
  my ($m, $duration, $sql) = @_;
  if ('ARRAY' ne ref $rh_session->{ 'queries' } ) {
    $rh_session->{ 'queries' } = [];
  }
  if (0 == scalar(@{$rh_session->{'queries'}})) { 
    $rh_session->{'time'} = $m;
    $rh_session->{'duration'} = $duration;
  }

  $rh_session->{ 'xact' } = 1 if $sql =~ $BEGIN_SQL;
  $rh_session->{ 'dml' } = 1 if $sql =~ $DML_SQL;

  push @{$rh_session->{'queries'}},[ $m, $duration, $sql ];
}


# 
# process_query
#
# When we get a complete query, append it and timing info to the list of queries
# for the given session. If the session is new, initialize it, with the 
# starting time, dbname; accumulate total duration.
# 
# If the query includes a commit string, split the query, flush the session
# 
sub process_query {
    my $d = shift;
    my $sid = $d->{_};
    return unless $sid;
    return unless $d->{ 'm' };
    return unless !scalar(@ANALYZE_DB) || grep { $_ eq $d->{ 'd' } } @ANALYZE_DB;
    my $sql = $d->{ 'sql' };

    # my $end_time = get_ms_from_timestamp( $d->{ 'm' } );
    # return unless $end_time;
    # my $beginning_time = $end_time - $d->{ 'duration' };

    if (!$sessions{ $sid } ) {
	$sessions{ $sid } = { };
	$sessions{ $sid }->{ 'dbname' } = $d->{ 'd' } if $d->{'d'};
	reset_session($sessions{ $sid }, $d->{ 'm' });
    }

    $sessions{ $sid }->{ 'duration' } += $d->{ 'duration' };

    # A commit might "Split" the current query 
    # For reporting, we output the queries per-transaction
    if ($sql =~ $COMMIT_SQL)  {
      my ($xact_prev, $xact_curr) = (prematch($sql) . match($sql), postmatch($sql));
      add_query( $sessions{$sid}, $d->{'m'}, $d->{'duration'}, $xact_prev);

      if (length($xact_curr)) {
	flush_session( $sid, 0 );
	add_query( $sessions{$sid}, $d->{'m'}, 0.0, $xact_curr);
      }
      else {
	flush_session( $sid, 1 );
      }
    }
    else { 
      add_query( $sessions{ $sid }, $d->{ 'm' }, $d->{ 'duration' }, $d->{ 'sql' } );
    }

    # I don't understand this code:
    ##  my $qr = qr{\A\s*(?:$COMMIT_SQL\s*;\s*)?$BEGIN_SQL\s*;?\s*\z};
    ## if ( $d->{ 'sql' } =~ m{\A\s*(?:$COMMIT_SQL\s*;\s*)?$BEGIN_SQL\s*;?\s*\z} ) {
    ##     $sessions{ $sid } = {
    ##         'last'    => $end_time,
    ##         'queries' => [ $d->{ 'sql' } ],
    ##     };
    ##     return;
    ## }

    return;
}

sub get_ms_from_timestamp {
    use DateTime;
    my $timestamp = shift;
    return unless $timestamp =~ m{^(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)(?:\.(\d{1,3}))?(?: (\S+))?};
    my $dt = DateTime->new( year=>$1,month=>$2,day=>$3,hour=>$4,minute=>$5,second=>$6,time_zone=>$8 );
    # Hack to add milliseconds. If we use nanoscconds, the bignum module returns a 
    # HASH which interferes with DateTime's validate() procedure, which expects a SCALAR
    return $dt->epoch() * 1000 + $7;
}
