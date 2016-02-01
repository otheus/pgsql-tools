#!/usr/bin/perl
use strict;

use File::Spec;
use Data::Dumper;
use POSIX qw(strftime);
use Fatal qw(open);
use bignum;

my $MINIMAL_IDLE_TO_REPORT = 1000;
#my ( $OPTIONAL_HEADER, @HEADER_ELEMENTS ) = get_optional_header_re( '%m %u@%d %p %r ' );
my ( $OPTIONAL_HEADER, @HEADER_ELEMENTS ) = get_optional_header_re( '%m\t%d\t%u\t%c\t%l\t%e\t' );
my $NO_HEADER  = '\t\t\t\t\t\t';
my $ANALYZE_DB = '*';
my $COMMIT_SQL = qr{(?:COMMIT|ROLLBACK|END|ABORT)}i;
my $BEGIN_SQL  = qr{(?:BEGIN|START)}i;

my %pids = ();
parse_stdin();

exit;

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

sub parse_stdin {
    my $last = {};
    my $read = 0;

    while ( my $line = <> ) {
        chomp $line;
        $read++;
        print STDERR '#' if 0 == $read % 1000;

        my @temp;
        my %prefix;
        if ( scalar( @temp = $line =~ m/$OPTIONAL_HEADER/ ) ) {
	  $line =~ s/$OPTIONAL_HEADER//;
	  @prefix{ @HEADER_ELEMENTS } = @temp;
	#} elsif ( $line =~ s/^\t\s*// ) {
	} else {
	    # that's ok. continuation line.
            next unless $last->{ 'time' };
            $last->{ 'sql' } .= ' ' . $line;
	    next;
	}

        if ( $line =~ m{ \A LOG: \s+ duration: \s+ (\d+\.\d+) \s+ ms \s+ ( statement | execute [^:]* ): \s+ (.*?) \s* \z }xms ) {

            my ( $time, $mode, $sql ) = ( $1, $2, $3 );
	    process_query( $last ) if $last->{ 'time' } ;
	    $last = {
		  'time'   => $time,
		  'sql'    => $sql,
		  'prefix' => \%prefix,
	    };
	    next;
        }
        elsif ( $line =~ m{ \A (?: LOG | STATEMENT | NOTICE | HINT | DETAIL | FATAL | WARNING | PANIC | ERROR ) : \s{1,2} }xms ) {
            process_query( $last ) if $last->{ 'time' };
            $last = {};
            next;
        }
        else {
	    warn "Unknown log line : $line";
        }
    }
    process_query( $last ) if $last->{ 'sql' };
    return;
}

sub process_query {
    my $d = shift;
    if (   ( $ANALYZE_DB )
        && ( $ANALYZE_DB ne '*' )
        && ( $d->{ 'prefix' }->{ 'd' } ne $ANALYZE_DB ) )
    {
        return;
    }
    return unless $d->{ 'prefix' }->{ 'm' };
    return unless ($d->{ 'prefix' }->{ 'p' } || $d->{ 'prefix' }->{ 'c' });

    my $end_time = get_ms_from_timestamp( $d->{ 'prefix' }->{ 'm' } );
    return unless $end_time;
    my $beginning_time = $end_time - $d->{ 'time' };

    my $pid = ($d->{ 'prefix' }->{ 'p' } || $d->{ 'prefix' }->{ 'c' });

    if ( $pids{ $pid } ) {
        my $previous_end = $pids{ $pid }->{ 'last' };
        if ( $beginning_time - $previous_end >= $MINIMAL_IDLE_TO_REPORT ) {
            printf( "%ums @ %s (pid: %s), queries:\n", $beginning_time - $previous_end, $d->{ 'prefix' }->{ 'm' }, $pid );
            print "  - $_\n" for ( @{ $pids{ $pid }->{ 'queries' } }, $d->{ 'sql' } );
            print "\n";
        }
    }
    if ( $d->{ 'sql' } =~ m{\A\s*$COMMIT_SQL\s*;?\s*\z} ) {
        delete $pids{ $pid };
        return;
    }
    my $qr = qr{\A\s*(?:$COMMIT_SQL\s*;\s*)?$BEGIN_SQL\s*;?\s*\z};
    if ( $d->{ 'sql' } =~ m{\A\s*(?:$COMMIT_SQL\s*;\s*)?$BEGIN_SQL\s*;?\s*\z} ) {
        $pids{ $pid } = {
            'last'    => $end_time,
            'queries' => [ $d->{ 'sql' } ],
        };
        return;
    }
    if ( $pids{ $pid } ) {
        $pids{ $pid }->{ 'last' } = $end_time;
        push @{ $pids{ $pid }->{ 'queries' } }, $d->{ 'sql' };
    }
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
